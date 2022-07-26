import json
from argparse import ArgumentParser
from glob import glob
import os

from generate_run_id import GenerateRunID
from loggers import get_logger
from environment import SCOPES_DIR
import workflows


logger = get_logger("MAIN")


def check_pid(pid: int):
    if pid is None:
        raise ValueError("--pid argument must be set to run partial pipeline")


if __name__ == '__main__':
    
    parser = ArgumentParser()

    parser.add_argument("--action", type=str, required=True)
    parser.add_argument("--n_parallel", type=int, required=False)
    parser.add_argument("--n_parallel_small", type=int, required=False)
    parser.add_argument("--n_parallel_medium", type=int, required=False)
    parser.add_argument("--n_parallel_large", type=int, required=False)
    parser.add_argument("--n_parallel_huge", type=int, required=False)
    parser.add_argument("--n_parallel_soldto", type=int, required=False)
    parser.add_argument("--pid", type=int, required=False)
    parser.add_argument("--max_scopes", type=int, required=False, default=0)
    parser.add_argument("--min_dims_medium", type=int, required=False)
    parser.add_argument("--max_dims_medium", type=int, required=False)
    parser.add_argument("--group", type=str, required=False)
    parser.add_argument("--incremental", action="store_true")

    args = parser.parse_args()

    valid_args = {k: v for k, v in vars(args).items() if v is not None}
    logger.info("TASK BEGINS", extra={"task_completed": False, **valid_args})

    if args.action == "initialize":

        # remove all scopefiles
        for file in glob(f"{SCOPES_DIR}/*"):
            os.remove(file)

        workflows.initialize.salesorder(
            incremental=args.incremental,
            n_parallel_small=args.n_parallel_small,
            n_parallel_medium=args.n_parallel_medium,
            n_parallel_large=args.n_parallel_large,
            n_parallel_huge=args.n_parallel_huge,
            min_dims_medium=args.min_dims_medium,
            max_dims_medium=args.max_dims_medium,
            max_scopes=args.max_scopes,
        )
        workflows.initialize.soldto(
            incremental=args.incremental,
            n_parallel=args.n_parallel_soldto,
            max_scopes=args.max_scopes,
        )
    elif args.action == "generate_run_id":
        generate = GenerateRunID.parse(None, True)
        run_dictionary = {"RUN_ID": generate.get_run_name(),
                          "RUN_SCHEMA_NAME": generate.get_schema_name()}
        with open('../../../airflow/xcom/return.json', 'w') as f:
            f.write(json.dumps(run_dictionary))

    elif args.action == "initialize_dnb":
        workflows.initialize.dnb(
            incremental=args.incremental,
            n_parallel_small=args.n_parallel_small,
            n_parallel_medium=args.n_parallel_medium,
            n_parallel_large=args.n_parallel_large,
            n_parallel_huge=args.n_parallel_huge,
            max_scopes=args.max_scopes,
        )

    elif args.action == "initialize_keepstock":
        workflows.initialize.keepstock(
            incremental=args.incremental,
            n_parallel=args.n_parallel,
            max_scopes=args.max_scopes,
        )

    elif args.action == "generate_ops_locations":
        check_pid(args.pid)
        if args.group == "small":
            workflows.generate.generate_ops_locations_small(
                pid=args.pid,
                group=args.group,
            )
        else:
            workflows.generate.generate_ops_locations(
                pid=args.pid,
                group=args.group,
            )
    
    elif args.action == "generate_soldto_ops_locations":
        workflows.generate.generate_ops_soldto_locations(
            pid=args.pid,
        )

    elif args.action == "generate_parent_ops_locations":
        check_pid(args.pid)
        workflows.generate.generate_parent_ops_locations(
            pid=args.pid,
            group=args.group
        )

    elif args.action == "build_associations":
        check_pid(args.pid)
        workflows.associate.build_associations(
            pid=args.pid,
            group=args.group,
        )

    elif args.action == "build_associations_soldto_account":
        check_pid(args.pid)
        workflows.associate.build_associations_soldto(
            pid=args.pid
        )

    elif args.action == "build_associations_dnb":
        workflows.associate.build_associations_dnb(
            group=args.group,
            pid=args.pid
        )

    elif args.action == "build_associations_keepstock":
        workflows.associate.build_associations_keepstock(
            pid=args.pid,
        )

    elif args.action == "commit_associations":
        workflows.commit.commit_associations("salesorder")

    elif args.action == "commit_associations_dnb":
        workflows.commit.commit_associations("dnb")

    elif args.action == "commit_associations_keepstock":
        workflows.commit.commit_associations("keepstock")

    elif args.action == "commit_associations_soldto_account":
        workflows.commit.commit_associations("soldto")

    elif args.action == "populate_bridge_table":
        workflows.commit.populate_bridge_table()

    elif args.action == 'compute_run_statistics':
        workflows.compute_stats.compute_stats_table()

    else:
        raise ValueError("Unknown Action")

    logger.info("TASK COMPLETE", extra={"task_completed": True, **valid_args})
