/**
 * LightGBM + XGBoost + CatBoost ensemble inference in TypeScript.
 * - LightGBM: tree traversal for win probability
 * - XGBoost: tree traversal for rank scores, softmax per race
 * - CatBoost: oblivious tree traversal for win probability
 * - Stacking meta-learner: logistic regression on base model outputs
 * - Platt scaling calibration
 */


interface PlattCalibration {
  weight: number
  bias: number
}

interface MetaLearner {
  weights: number[]
  intercept: number
  feature_names: string[]
}

interface ExportedModel {
  version: string
  feature_names: string[]
  // v4: 3-way ensemble weights
  ensemble_weights?: { lgbm: number; xgb: number; catboost: number }
  // v3 backward compat
  ensemble_weight_lgbm?: number
  medians: Record<string, number>
  lgbm_model: {
    tree_info: Array<{ tree_structure: any }>
  }
  platt_calibration: PlattCalibration | null
  meta_learner: MetaLearner | null
  // Legacy v1/v2/v3 format support
  calibration?: { x: number[]; y: number[] } | null
  model?: {
    tree_info: Array<{ tree_structure: any }>
  }
}

interface XGBTreeNode {
  nodeid: number
  split?: string
  split_condition?: number
  yes?: number
  no?: number
  missing?: number
  children?: XGBTreeNode[]
  leaf?: number
}

interface CatBoostTree {
  splits: Array<{
    float_feature_index: number
    border: number
  }>
  leaf_values: number[]
}

interface CatBoostModel {
  oblivious_trees: CatBoostTree[]
  model_info?: {
    params?: {
      data_processing_options?: {
        float_features_binarization?: any
      }
    }
  }
}

function sigmoid(x: number): number {
  return 1 / (1 + Math.exp(-x))
}

// LightGBM tree traversal
function traverseLGBM(node: any, features: number[]): number {
  if (node.leaf_value !== undefined) return node.leaf_value
  const val = features[node.split_feature]
  if (val === null || val === undefined || isNaN(val)) {
    return traverseLGBM(node.left_child, features)
  }
  return val <= node.threshold
    ? traverseLGBM(node.left_child, features)
    : traverseLGBM(node.right_child, features)
}

// XGBoost tree traversal
function traverseXGB(node: XGBTreeNode, features: number[], featureMap: Map<string, number>): number {
  if (node.leaf !== undefined) return node.leaf
  if (!node.split || !node.children) return 0

  const featureIdx = featureMap.get(node.split) ?? -1
  const val = featureIdx >= 0 ? features[featureIdx] : NaN

  if (isNaN(val) || val === null || val === undefined) {
    const missingId = node.missing ?? node.yes!
    const child = node.children.find(c => c.nodeid === missingId)
    return child ? traverseXGB(child, features, featureMap) : 0
  }

  const nextId = val < node.split_condition! ? node.yes! : node.no!
  const child = node.children.find(c => c.nodeid === nextId)
  return child ? traverseXGB(child, features, featureMap) : 0
}

// CatBoost oblivious tree traversal
function traverseCatBoost(tree: CatBoostTree, features: number[]): number {
  let leafIdx = 0
  const splits = tree.splits
  for (let depth = splits.length - 1; depth >= 0; depth--) {
    const split = splits[depth]
    const val = features[split.float_feature_index]
    // CatBoost: if val > border, go right (bit = 1)
    if (val > split.border) {
      leafIdx |= (1 << (splits.length - 1 - depth))
    }
  }
  return tree.leaf_values[leafIdx] ?? 0
}

export class LGBMPredictor {
  private model: ExportedModel
  private featureNames: string[]
  private xgbTrees: XGBTreeNode[] | null = null
  private xgbFeatureMap: Map<string, number> = new Map()
  private catboostTrees: CatBoostTree[] | null = null
  private medians: Record<string, number>

  constructor(modelJson: ExportedModel, xgbJson?: any, catboostJson?: any) {
    this.model = modelJson
    this.featureNames = modelJson.feature_names
    this.medians = modelJson.medians || {}

    if (xgbJson?.learner?.gradient_booster?.model?.trees) {
      this.xgbTrees = xgbJson.learner.gradient_booster.model.trees
      this.featureNames.forEach((name, i) => {
        this.xgbFeatureMap.set(name, i)
      })
    }

    if (catboostJson?.oblivious_trees) {
      this.catboostTrees = catboostJson.oblivious_trees
    }
  }

  get version(): string {
    return this.model.version
  }

  get ensembleWeights(): { lgbm: number; xgb: number; catboost: number } {
    if (this.model.ensemble_weights) {
      return this.model.ensemble_weights
    }
    // v3 backward compat
    const lgbm = this.model.ensemble_weight_lgbm ?? 1.0
    return { lgbm, xgb: 1 - lgbm, catboost: 0 }
  }

  private toFeatureArray(features: Record<string, number | null>): number[] {
    return this.featureNames.map(name => {
      const val = features[name]
      if (val === null || val === undefined) return this.medians[name] ?? NaN
      return val
    })
  }

  // LightGBM probability
  predictLGBMProb(features: Record<string, number | null>): number {
    const arr = this.toFeatureArray(features)
    const trees = this.model.lgbm_model?.tree_info ?? this.model.model?.tree_info ?? []
    let logit = 0
    for (const tree of trees) {
      logit += traverseLGBM(tree.tree_structure, arr)
    }
    return sigmoid(logit)
  }

  // XGBoost rank score
  predictXGBScore(features: Record<string, number | null>): number {
    if (!this.xgbTrees) return 0
    const arr = this.toFeatureArray(features)
    let score = 0
    for (const tree of this.xgbTrees) {
      score += traverseXGB(tree, arr, this.xgbFeatureMap)
    }
    return score
  }

  // CatBoost probability
  predictCatBoostProb(features: Record<string, number | null>): number {
    if (!this.catboostTrees) return 0
    const arr = this.toFeatureArray(features)
    let logit = 0
    for (const tree of this.catboostTrees) {
      logit += traverseCatBoost(tree, arr)
    }
    return sigmoid(logit)
  }

  // Platt scaling calibration
  private plattCalibrate(prob: number): number {
    const platt = this.model.platt_calibration
    if (!platt) return prob
    return sigmoid(platt.weight * prob + platt.bias)
  }

  // Stacking meta-learner prediction
  private metaPredict(
    lgbmProb: number, xgbProb: number, catboostProb: number,
    odds: number, fieldSize: number, isFavourite: number
  ): number {
    const meta = this.model.meta_learner
    if (!meta) return 0
    const features = [
      lgbmProb, xgbProb, catboostProb,
      (isNaN(odds) ? 10 : odds) / 100.0,
      fieldSize / 20.0,
      isFavourite,
    ]
    let logit = meta.intercept
    for (let i = 0; i < meta.weights.length && i < features.length; i++) {
      logit += meta.weights[i] * features[i]
    }
    return sigmoid(logit)
  }

  // Single runner prediction (backward compatible)
  predictCalibrated(features: Record<string, number | null>): number {
    const prob = this.predictLGBMProb(features)
    return this.plattCalibrate(prob)
  }

  // Predict for a whole race with 3-model ensemble
  predictRace(
    runners: Array<{ horseId: string; features: Record<string, number | null> }>
  ): Array<{ horseId: string; winProb: number; rank: number }> {
    const w = this.ensembleWeights

    const lgbmProbs = runners.map(r => this.predictLGBMProb(r.features))

    let ensembleProbs: number[]

    const hasXgb = this.xgbTrees && w.xgb > 0
    const hasCatboost = this.catboostTrees && w.catboost > 0

    if (hasXgb || hasCatboost) {
      // XGBoost scores -> softmax per race
      let xgbProbs: number[]
      if (hasXgb) {
        const xgbScores = runners.map(r => this.predictXGBScore(r.features))
        const maxScore = Math.max(...xgbScores)
        const expScores = xgbScores.map(s => Math.exp(s - maxScore))
        const sumExp = expScores.reduce((a, b) => a + b, 0)
        xgbProbs = expScores.map(e => e / sumExp)
      } else {
        xgbProbs = lgbmProbs.map(() => 0)
      }

      // CatBoost probabilities
      let catboostProbs: number[]
      if (hasCatboost) {
        catboostProbs = runners.map(r => this.predictCatBoostProb(r.features))
      } else {
        catboostProbs = lgbmProbs.map(() => 0)
      }

      // Check if we have a meta-learner (stacking)
      if (this.model.meta_learner) {
        ensembleProbs = runners.map((r, i) => {
          const odds = r.features['current_odds'] ?? 10
          const fieldSize = r.features['field_size'] ?? 10
          const isFav = r.features['is_favourite'] ?? 0
          return this.metaPredict(
            lgbmProbs[i], xgbProbs[i], catboostProbs[i],
            odds, fieldSize, isFav
          )
        })
      } else {
        // Simple weighted average
        ensembleProbs = lgbmProbs.map((lp, i) =>
          w.lgbm * lp + w.xgb * xgbProbs[i] + w.catboost * catboostProbs[i]
        )
      }
    } else {
      ensembleProbs = lgbmProbs
    }

    // Normalize per-race (sum to 1)
    const total = ensembleProbs.reduce((a, b) => a + b, 0)
    const normalized = total > 0
      ? ensembleProbs.map(p => p / total)
      : ensembleProbs

    const predictions = runners.map((r, i) => ({
      horseId: r.horseId,
      winProb: Math.max(0.001, Math.min(0.999, normalized[i])),
      rank: 0,
    }))

    predictions.sort((a, b) => b.winProb - a.winProb)
    predictions.forEach((p, i) => { p.rank = i + 1 })

    return predictions
  }
}

export function loadModel(modelJson: unknown, xgbJson?: unknown, catboostJson?: unknown): LGBMPredictor {
  return new LGBMPredictor(modelJson as ExportedModel, xgbJson, catboostJson)
}
