import logging


class EventPacher(object):
    """h"""

    def __init__(self, core):
        self.core = core
        self.tmp_events = {}
        self.torrent_manager = None

    def patch_events(self):
        """patch"""
        # get cores...
        torrent_manager = self.core.torrentmanager
        if not torrent_manager:
            logging.warn("Cannot patch events, torrent_manager is null.")
            return
        # on_alert_file_completed
        self.torrent_manager = torrent_manager
        self.tmp_events['on_alert_file_completed'] = torrent_manager.on_alert_file_completed
        torrent_manager.on_alert_file_completed = self.on_alert_file_completed

        self.tmp_events['on_alert_torrent_finished'] = torrent_manager.on_alert_torrent_finished
        torrent_manager.on_alert_torrent_finished = self.on_alert_torrent_finished

        self.tmp_events['on_alert_storage_moved'] = torrent_manager.on_alert_storage_moved
        torrent_manager.on_alert_storage_moved = self.on_alert_storage_moved

        self.tmp_events['on_alert_torrent_finished'] = torrent_manager.on_alert_torrent_finished
        torrent_manager.on_alert_torrent_finished = self.on_alert_torrent_finished

        # on_alert_storage_moved_failed
        self.tmp_events['on_alert_storage_moved_failed'] = torrent_manager.on_alert_storage_moved_failed
        torrent_manager.on_alert_storage_moved_failed = self.on_alert_storage_moved_failed

        logging.info("all events patched.")

        # TorrentAddedEvent already have, ignore...
        # TorrentFinishedEvent already have
        # on_alert_torrent_paused no need
        # on_alert_torrent_checked not use?
        # on_alert_tracker_reply no need.
        # if move emit TorrentFinishedEvent
        # on_alert_torrent_resumed
        # on_alert_state_changed
        # on_alert_save_resume_data
        # on_alert_save_resume_data_failedon_alert_file_completed
        # on_alert_fastresume_rejected
        # on_alert_file_renamed TorrentFileRenamedEvent??
        # on_alert_metadata_received
    def on_alert_file_completed(self, alert):
        """path on_alert_file_completed"""
        logging.info("**************************")
        # self.tmp_events['on_alert_file_completed'](alert)
        logging.info("@@@@@@@@@@@@@@@@@@@@@@@@@@")

    def on_alert_torrent_finished(self, alert):
        """patch on_alert_torrent_finished"""
        logging.info("oooooooooooooooooooooooooo")
        self.tmp_events['on_alert_torrent_finished'](alert)
        logging.info("ffffffffffffffffffffffffff")

    def on_alert_torrent_finished(self, alert):
        """patch on_alert_torrent_finished"""
        logging.info("oooooooooooooooooooooooooo on_alert_torrent_finished")
        self.tmp_events['on_alert_torrent_finished'](alert)
        logging.info("ffffffffffffffffffffffffff")

    def on_alert_storage_moved_failed(self, alert):
        """patch on_alert_storage_moved_failed"""
        logging.info(
            "oooooooooooooooooooooooooo on_alert_storage_moved_failed")
        self.tmp_events['on_alert_storage_moved_failed'](alert)
        logging.info("ffffffffffffffffffffffffff")
