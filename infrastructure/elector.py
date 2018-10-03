import time

from .. import ZooKeeper


class SchedulerElector(ZooKeeper):
    SCHEDULER_ELECTION_PATH = '/*/processing/election'

    def __init__(self, identity: str, runnable) -> None:
        super().__init__()

        self.runnable = runnable
        self.identity = identity

        self._election()

    def _election(self):
        print('Running:', self.identity)
        election = self._zk.Election(self.SCHEDULER_ELECTION_PATH, identifier=self.identity)

        # call will block until this scheduler has won the election (raises as the next leader)
        # once this process is the leader we can start our 'normal' thread loop
        election.run(self.run)

    def run(self):
        print('Elected:', self.identity)
        while True:
            try:
                self.runnable.run()
            # TODO: this should be a custom exception that stops the infinite loop
            except Exception as e:
                raise

            time.sleep(1)
