"""
.. See the NOTICE file distributed with this work for additional information
   regarding copyright ownership.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import configparser
import os


class ConfigHandler(object):

    __instance = None

    @staticmethod
    def getInstance():
        """static access method"""
        if ConfigHandler.__instance is None:
            ConfigHandler()
        return ConfigHandler.__instance

    def __init__(self, ini_file=None):
        if ini_file is None:
            self.ini_file = os.path.abspath(
                os.path.dirname(__file__)
            ) + "/../../../conf/refseq_source.ini"
        else:
            self.ini_file = ini_file

        config = configparser.ConfigParser()
        config.read(self.ini_file)
        self.config = config
        ConfigHandler.__instance = self

    def get_section_config(self, section_name=None):
        if section_name is None:
            config = self.config['DEFAULT']
        else:
            config = self.config[section_name]
        return config
