import logging


class EventPacher(object):
    """h"""

    def __init__(self, core):
        self.core = core
        self.tmp_events = {}

    def patch_events(self):
        """patch"""
        # get cores...
        torrent_manager = self.core.torrentmanager
        if not torrent_manager:
            logging.warn("Cannot patch events, torrent_manager is null.")
            return
        # TorrentAddedEvent already have, ignore...
        # TorrentFinishedEvent already have
        # on_alert_torrent_paused no need
        # on_alert_torrent_checked not use?
        # on_alert_tracker_reply no need.
        # if move emit TorrentFinishedEvent
        # on_alert_torrent_resumed
        # on_alert_state_changed
        # on_alert_save_resume_data
        # on_alert_save_resume_data_failed
        # on_alert_fastresume_rejected
        # on_alert_file_renamed TorrentFileRenamedEvent??
        # on_alert_metadata_received
