import os
from xcalar.solutions.controller import Controller
from structlog import get_logger

logger = get_logger(__name__)


class Singleton_Controller(object):
    __instance = None

    @staticmethod
    def getInstance():
        """ Static access method. """
        if Singleton_Controller.__instance is None:
            Singleton_Controller()
        return Singleton_Controller.__instance

    def __init__(self, properties=None, universe_id=None, session_name=None):
        """ Virtually private constructor. """
        # self.controller = Controller()

        self.controller = Controller()
        Singleton_Controller.__instance = self
        if properties is None:
            properties = os.environ.get("XCALAR_SHELL_PROPERTIES_FILE")
        if universe_id is None:
            universe_id = os.environ.get("XCALAR_SHELL_UNIVERSE_ID")
        if session_name is None:
            session_name = os.environ.get("XCALAR_SHELL_SESSION_NAME")
        else:
            session_name = universe_id

        self.controller.initialize(properties, universe_id, session_name)

        if "XCALAR_SHELL_PROPERTIES_FILE" in os.environ:
            logger.info(
                f'Properties file: {os.environ.get("XCALAR_SHELL_PROPERTIES_FILE")}'
            )
        if "XCALAR_SHELL_UNIVERSE_ID" in os.environ:
            logger.info(
                f'Universe ID: {os.environ.get("XCALAR_SHELL_UNIVERSE_ID")}')
        if "XCALAR_SHELL_SESSION_NAME" in os.environ:
            logger.info(
                f'Session Name: {os.environ.get("XCALAR_SHELL_SESSION_NAME")}')

        try:
            logger.info(
                f"Restoring orchestrator from session: {self.controller.session.name}"
            )
            self.controller.restoreOrchestrator(skip_props_refresh=False)
            logger.info(
                f"Connected to {self.controller.orchestrator.dispatcher.session.name} at {self.controller.orchestrator.dispatcher.xcalarClient._url}"
            )
        except Exception as e:
            logger.warning(
                f"Failed to restore state from checkpoint\n{str(e)}")
            logger.warning(f"Please run controller -snapshot_recover\n\n")
            pass

    def get_controller(self):
        return self.controller


##
# Singleton servos driver:
##
if __name__ == "__main__":
    singleton_controller_1 = Singleton_Controller()
    print(singleton_controller_1)
    singleton_controller_2 = Singleton_Controller.getInstance()
    print(singleton_controller_2)
