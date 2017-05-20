import logging
from deluge._libtorrent import lt


class EventPacher(object):
    """h"""

    def __init__(self, core):
        self.core = core
        self.tmp_events = {}
        self.torrent_manager = None

    def patch_events(self):
        """patch"""
        # get cores...
        alert_mask = (lt.alert.category_t.error_notification |
                      lt.alert.category_t.port_mapping_notification |
                      lt.alert.category_t.storage_notification |
                      lt.alert.category_t.tracker_notification |
                      lt.alert.category_t.status_notification |
                      lt.alert.category_t.ip_block_notification |
                      lt.alert.category_t.performance_warning |
                      lt.alert.category_t.progress_notification)
        torrent_manager = self.core.torrentmanager
        if not torrent_manager:
            logging.warn("Cannot patch events, torrent_manager is null.")
            return
        # on_alert_file_completed
        torrent_manager.alerts.session.apply_settings(
            {'alert_mask': alert_mask})
        self.torrent_manager = torrent_manager
        """
        torrent_manager.alerts.register_handler(
            'torrent_finished_alert', self.on_alert_torrent_finished)
        torrent_manager.alerts.register_handler(
            'file_completed_alert', self.on_alert_file_completed)
        """
        torrent_manager.alerts.register_handler(
            'metadata_received_alert', self.on_alert_metadata_received
        )
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
        logging.info("**************************on_alert_file_completed")
        # self.tmp_events['on_alert_file_completed'](alert)
        logging.info("@@@@@@@@@@@@@@@@@@@@@@@@@@")

    def on_alert_torrent_finished(self, alert):
        """patch on_alert_torrent_finished"""
        logging.info("ooooooooooooooooooooooooooon_alert_torrent_finished")
        logging.info("ffffffffffffffffffffffffff")

    def on_alert_metadata_received(self, alert):
        """patch on_alert_metadata_received"""
        logging.info("ooooooooooooooooooooooooooon_metadata_received")
        logging.info("ffffffffffffffffffffffffff")

    def on_alert_storage_moved(self, alert):
        """patch on_alert_torrent_finished"""
        logging.info("oooooooooooooooooooooooooo on_alert_storage_moved")
        logging.info("ffffffffffffffffffffffffff")

    def on_alert_storage_moved_failed(self, alert):
        """patch on_alert_storage_moved_failed"""
        logging.info(
            "oooooooooooooooooooooooooo on_alert_storage_moved_failed")
        logging.info("ffffffffffffffffffffffffff")
