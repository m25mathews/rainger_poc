import pandas as pd
import numpy as np
import os
from hyperopt import hp
from argparse import ArgumentParser
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import LinearSVC
from sklearn.dummy import DummyClassifier
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.metrics import f1_score, make_scorer
from sklearn.naive_bayes import ComplementNB, MultinomialNB
from lightgbm import LGBMClassifier
from copy import deepcopy
from hyperopt import fmin, tpe, hp, Trials, STATUS_FAIL, STATUS_OK
import warnings
import pickle
from pprint import pprint
from hyperopt.pyll.stochastic import sample as sample_from
from tqdm import tqdm

from vectorizers import WordNGramTfidfVectorizer

from loggers import get_logger

logger = get_logger("TUNING_WIZARD")

# for reproducibility
os.environ["HYPEROPT_FMIN_SEED"] = "3141592654"

# Dummy pipeline that will be copied and parameters reset
BASE_PIPELINE = Pipeline([
    ("featurizer", WordNGramTfidfVectorizer()),
    ("estimator", DummyClassifier()),
])

# class-name -> class map
model_classes = {
    c.__name__: c
    for c in [
        KNeighborsClassifier,
        LinearSVC,
        LGBMClassifier,
        ComplementNB,
        MultinomialNB
    ]
}

def load_datasets(debug=False, path="../../data/work", min_rows=100):
    """Loads all training sets in `path`. Files should be xlsx format
    and prefixed with TRN_

    Args:
        debug (bool, optional): debug mode reads only one file. Defaults to False.
        path (str, optional): Path to training files. Defaults to "../../data/work".
        min_rows(int, optional): Minimum training rows to use; files not satisfying this are ignored.
            Defaults to 100
    Returns:
        Dict[str, DataFrame]: mapping of file name (no suffix) to dataframe instance

    """
    files = [*filter(
        lambda s: s.startswith("TRN_") and s.endswith(".xlsx"),
        os.listdir(path)
    )]
    if debug:
        files = files[:1]
    frames = {
        file.split(".")[0].split("/")[-1]: pd.read_excel(f"{path}/{file}")
        for file in files
    }
    
    return {
        key: val
        for key, val in frames.items()
        if len(val) >= min_rows
    }

# set up training sets and parameter search space
DATA_FRAMES = load_datasets()

SEARCH_SPACE = {
    "featurizer": {
        # words
        "words_weight": hp.uniform("words_tfidf_weight", 0.1, 1.0),
        "words_min_df": hp.choice("words_mdf", [0.0, hp.uniform("words_tfidf_min_df", 0.01, 0.2)]),
        "words_max_df": hp.choice("words_xdf", [0.0, hp.uniform("words_tfidf_max_df", 0.8, 1.0)]),
        "words_ngram_range": hp.choice("words_tfidf_ngram", [(1,1),(1,2)]),
        "words_use_idf": hp.choice("words_tfidf_idf", [True, False]),
        "words_smooth_idf": hp.choice("words_tfidf_smooth", [True, False]),
        # ngrams
        "ngrams_weight": hp.uniform("ngrams_tfidf_weight", 0.1, 1.0),
        "ngrams_min_df": hp.choice("ngrams_mdf", [0.0, hp.uniform("ngrams_tfidf_min_df", 0.0, 0.2)]),
        "ngrams_max_df": hp.choice("ngrams_xdf", [0.0, hp.uniform("ngrams_tfidf_max_df", 0.8, 1.0)]),
        "ngrams_ngram_range": hp.choice("ngrams_tfidf_ngram", [
            (2,2), (2,3), (3,3), (2,4), (3,4), (4,4)
        ]),
        "ngrams_use_idf": hp.choice("ngrams_tfidf_idf", [True, False]),
        "ngrams_smooth_idf": hp.choice("ngrams_tfidf_smooth", [True, False])
    },
    "estimator": hp.choice('classifier', [
        # {
        #     'type': "LinearSVC",
        #     "C": hp.loguniform("lsvc_c", -3.5, 1.5),
        #     "penalty": hp.choice("lsvc_penalty", ["l1", "l2"]),
        #     "loss": hp.choice("lsvc_loss", ["hinge", "squared_hinge"]),
        #     "class_weight": hp.choice("lsvc_class_weight", [None, "balanced"]),
        # },
        {
            'type': 'KNeighborsClassifier',
            'metric': hp.choice('kneigh_metric', ["minkowski", "cosine"]),
            "n_neighbors": hp.quniform("kneigh_k", 1, 5, 1),
            "n_jobs": -1
        },
        # {  # big problems with the classes with low cardinality here
        #     'type': 'LGBMClassifier',
        #     "objective": "multiclass",
        #     "class_weight": hp.choice("gbc_class_weight", [None, "balanced"]),
        #     "colsample_bytree": hp.uniform("gbc_cols", 0.5, 1.0),
        #     "reg_lambda": hp.loguniform("gbc_lambda", -4, 1),
        #     "reg_alpha": hp.loguniform("gbc_alpha", -4, 1),
        #     "n_estimators": hp.qlognormal("gbc_n_estimators", 4.3, 0.6, 1),
        #     'max_depth': hp.qlognormal('gbc_max_depth_int', 1.25, 0.4, 1),
        #     'min_child_samples': hp.quniform('gbc_min_child_samples', 10, 40, 1),
        #     "n_jobs": 3
        # },
        # {
        #     "type": "ComplementNB",
        #     "alpha": hp.loguniform('nbc_alp', -5, 1)
        # },
        # {
        #     "type": "MultinomialNB",
        #     "alpha": hp.loguniform('nbm_alp', -5, 1)
        # },
    ])
}


def make_model(samp):
    """Take a sample form the search space and
    convert it into a Pipeline instance

    Args:
        samp (dict): hyperparameters in the format:
            ```{'estimator': {'alpha': 0.22852870129929528, 'type': 'ComplementNB'},
                'featurizer': {'analyzer': 'char',
                    'max_df': 0.946853852925893,
                    'min_df': 0.029684606045513907,
                    'ngram_range': (2, 2)}}```

    Returns:
        Pipeline: sklearn pipeline built from `samp`
    """
    est = deepcopy(samp["estimator"])
    feat = samp["featurizer"]
    try:
        kind = est.pop("type")
    except KeyError:
        logger.error(samp)
        raise
    for k, v in est.items():
        if isinstance(v, float) and int(v) == v:
            est[k] = int(v)
    estimator = model_classes.get(kind)(**est)
    
    return deepcopy(BASE_PIPELINE).set_params(**{
        "estimator": estimator,
        **{
            f"featurizer__{k}": v
            for k, v in feat.items()
        },
    })

def evaluate(sample):
    """Evaluate a model given hyperparameter `sample`

    Args:
        sample (dict): Hyperparameter sample- see `make_model`

    Returns:
        dict: dict of status, scores, overall loss, etc.
    """
    model = make_model(sample)
    return {
        "kind": sample["estimator"]["type"],
        "params": sample,
        **evaluate_model(model)
    }


# TODO: REFACTOR
# set scorer (this is ugly)
scorer = make_scorer(f1_score, average='weighted')
def eval_one(model, df):
    """Compute cross-validation scores across a single dataset
        Note: sets `num_classes` parameter for gradient boosters
    """
    if model.named_steps["estimator"].__class__.__name__ in ["LGBMClassifier", "XGBRegressor"]:
        model = model.set_params(
            estimator__num_classes=len(df["ID_y"].unique())
        )
    
    return np.mean(cross_val_score(
        model,
        df["DIM_STR_S"],
        df["ID_y"],
        cv=StratifiedKFold(n_splits=5),
        scoring=scorer,
        n_jobs=5,
    ))

def evaluate_model(model):
    """Evaluate a pre-built model Pipeline instance.
    Assumes architecture with an `estimator` step.

    Args:
        model (Pipeline): model Pipeline to evaluate
    """
    # compute cross val scores for all training sets
    scores = {
        k: eval_one(model, df)
        for k, df in DATA_FRAMES.items()
    }

    # overall loss is the median
    # minimize negative == maximize
    overall = -1 * np.average(
        list(scores.values()),
        weights=[len(df) for df in DATA_FRAMES.values()]
    )
    
    return {
        "loss": overall,
        "status": STATUS_OK,
        "scores": scores,
    }


def bayesopt(n_trials: int):
    """Minimize the overall loss using TPE and
    sampling from `SEARCH_SPACE`

    Args:
        n_trials (int): number of trials to perform
    """
    trials = Trials()
    fmin(
        evaluate,
        space=SEARCH_SPACE,
        algo=tpe.suggest,
        max_evals=n_trials,
        trials=trials,
    )
    best = trials.best_trial["result"]

    with open("bayesopt_trials.pkl", "wb") as f:
        pickle.dump(trials, f)

    pprint(best)


def randsearch(n_trials: int):
    """Perform a randomized hyperparameter search

    Args:
        n_trials (int): number of random trials to perform

    note: appends to `random_search_results.csv`
    """
    results = list()
    for i in tqdm(range(n_trials)):
        samp = sample_from(SEARCH_SPACE)
        result = evaluate(samp)
        
        row = pd.Series(result)[[
            "loss",
            "kind",
            "scores",
            "params"
        ]].to_frame().T
        results.append(row)
        row.to_csv(
            "random_search_results.csv",
            mode="a",
            header=i==0,
            index=False
        )
    
    results = pd.concat(results)
    results.loss = results.loss.astype(float)

    with open("randsearch_trials.pkl", "wb") as f:
        pickle.dump(results, f)
    
    best = results.loc[results.loss.idxmin()]
    pprint(best.iloc[0].to_dict())
    
    return results

if __name__ == '__main__':
    
    parser = ArgumentParser()
    parser.add_argument("--method", type=str)
    parser.add_argument("--n_trials", type=int)

    args = parser.parse_args()

    if args.n_trials <= 0:
        raise ValueError("n_trials must be > 0")
    
    warnings.filterwarnings("ignore")
    if args.method == "random":
        randsearch(args.n_trials)
    elif args.method == "bayes":
        bayesopt(args.n_trials)
    else:
        raise ValueError("method must be 'random' or 'bayes'")
