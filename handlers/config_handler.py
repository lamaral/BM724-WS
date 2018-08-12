import yaml
import sys
import os


class ConfigHandler:
    # Le arquivo de configuracao para obter em que porta HTTP escutar
    config_file_stream = open(os.path.join(sys.path[0], "config.yaml"), "r")
    config = yaml.load(config_file_stream)
