from deluge.event import DelugeEvent
# Extends Events...


class TorrentFileUploadCompletedEvent(DelugeEvent):
    """
    Emitted when a file completes.
    """

    def __init__(self, torrent_id, index, file_props):
        """
        :param torrent_id: the torrent_id
        :type torrent_id: string
        :param index: the file index
        :type index: int
        :param file_props: the file file_props
        :type file_props: dict
        """
        self._args = [torrent_id, index, file_props]
