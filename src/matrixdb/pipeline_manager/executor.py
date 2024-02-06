'''
Executor which executes a pipeline step or multiple steps
'''
import importlib
import json
import time
import argparse
import logging


def execute(pipeline_name, config):
    # Load pipeline function
    pipeline_module_name = config["pipelines"][pipeline_name]
    pipeline_module = importlib.import_module(pipeline_module_name)
    pipeline_method = getattr(pipeline_module, 'execute')
    pipeline_method(config)


def main():

    parser = argparse.ArgumentParser()

    # Define a command-line argument for the method name
    parser.add_argument("--pipeline", help="Name of the pipeline to execute")
    parser.add_argument("--config", help="Configuration file for the pipeline")

    # Parse the command-line arguments
    args = parser.parse_args()

    pipeline_name = args.pipeline
    config_file_name = args.config

    # Load config
    with open(config_file_name) as config_file:
        config = json.load(config_file)

    if pipeline_name not in config["pipelines"]:
        logging.error({
            "message": f'{pipeline_name} not a valid pipeline'
        })
        logging.info(f'Invalid pipelines: to be from the list {str(config["pipelines"]).keys()}')
        return

    # Configure logging
    logging.basicConfig(level=logging.INFO, handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'{config["log"]["location"]}/{pipeline_name}_{time.time()}.log')])

    execute(pipeline_name, config)


if __name__ == "__main__":
    main()

