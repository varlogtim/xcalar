import json
from types import SimpleNamespace


class PropertiesAdapter():
    def __init__(self, propertiesFilePath):
        with open(propertiesFilePath, 'r') as f:
            self.properties = SimpleNamespace(**json.load(f))

    def getXcalarProperties(self):
        return SimpleNamespace(**self.properties.xcalar)

    def getGitProperties(self):
        key = 'git'
        if key not in self.properties.__dict__:
            raise Exception('Git properties not found in properties file')
        return SimpleNamespace(**self.properties.git)

    def getKafkaServerTopicMap(self):
        return self.properties.kafka['serverTopicMap']

    def getKafkaClusterProps(self):
        return self.properties.kafka['clusterProps']

    def getCallbacksPluginName(self):
        key = 'callbacksPluginName'
        if key in self.properties.__dict__:
            return self.properties.callbacksPluginName
        return 'xcalar_callbacks'

    def getUniverseAdapterPluginName(self):
        key = 'universeAdapterPluginName'
        if key in self.properties.__dict__:
            return self.properties.universeAdapterPluginName
        return 'xcalar_universe_adapter'

    def getLoggingPropertiesFile(self):
        key = 'loggingPropertiesFilepath'
        if key in self.properties.__dict__:
            return self.properties.loggingPropertiesFilepath
        return None

    def getControllerPausePollFreqSecondss(self):
        key = 'controllerPausePollFreqSeconds'
        if key in self.properties.__dict__:
            return self.properties.controllerPausePollFreqSeconds
        return None

    def getControllerAppProps(self):
        return SimpleNamespace(**self.properties.controllerApp)
