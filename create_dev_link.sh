#!/bin/bash
cd /root/plugin/clusterautodownloadplugin
mkdir temp
export PYTHONPATH=./temp
/usr/bin/python setup.py build develop --install-dir ./temp
cp ./temp/ClusterAutoDownloadPlugin.egg-link /root/.config/deluge/plugins
rm -fr ./temp
