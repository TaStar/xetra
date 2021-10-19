"""Running the Xetra ETL application"""
import logging
import logging.config

import yaml

def main():    
    """
     entry point to run the xetra ETL job
    """
    # Parsing YAML file
    config_path= 'c:/Data Analyst study/Pipelines Python AWS Docker Course/Project_pipeline/xetra/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config) #load config as a dictanary
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")
    

if __name__=='__main__':


    
    main()