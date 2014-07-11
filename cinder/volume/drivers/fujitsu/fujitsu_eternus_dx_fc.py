# Copyright (c) 2014 FUJITSU LIMITED
# Copyright (c) 2012 EMC Corporation.
# Copyright (c) 2012 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

'''
FibreChannel Cinder Volume driver for Fujitsu ETERNUS DX S2 and S3 series.
'''

##------------------------------------------------------------------------------------------------##
##                                                                                                ##
##  ETERNUS OpenStack Volume Driver                                                               ##
##                                                                                                ##
##  Note      :                                                                                   ##
##  File Name : fujitsu_eternus_dx_fc.py                                                          ##
##  Copyright 2014 FUJITSU LIMITED                                                                ##
##                                                                                                ##
##  history :                                                                                     ##
##      2014.03 : 1.0.0 : volume(create,delete,attach,detach,create from snapshot)                ##
##                        snapshot(create,delete)                                                 ##
##      2014.04 : 1.0.1 : Fix comment                                                             ##
##------------------------------------------------------------------------------------------------##


from oslo.config import cfg
from cinder.openstack.common import log as logging
from cinder import exception
from cinder.volume import driver
from cinder.openstack.common import lockutils
from cinder.volume.drivers import fujitsu_eternus_dx_common
import time

LOG = logging.getLogger(__name__)

#--------------------------------------------------------------------------------------------------#
# Class : FJDXFCDriver                                                                             #
#         summary      : fibrechannel cinder volume driver for Fujitsu ETERNUS DX                  #
#--------------------------------------------------------------------------------------------------#
class FJDXFCDriver(driver.FibreChannelDriver):
    '''
    ETERNUS Cinder Volume Driver version1.0
    for Fujitsu ETERNUS DX S2 and S3 series
    '''

    #----------------------------------------------------------------------------------------------#
    # Method : __init__                                                                            #
    #         summary      :                                                                       #
    #         parameters   :                                                                       #
    #         return-value :                                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def __init__(self, *args, **kwargs):
        '''
        Constructor
        '''
        super(FJDXFCDriver, self).__init__(*args, **kwargs)
        self.common = fujitsu_eternus_dx_common.FJDXCommon(
            'fc',
            configuration=self.configuration)
        return

    #----------------------------------------------------------------------------------------------#
    # Method : check_for_setup_error                                                               #
    #         summary      :                                                                       #
    #         parameters   :                                                                       #
    #         return-value :                                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def check_for_setup_error(self):
        pass
        return

    #----------------------------------------------------------------------------------------------#
    # Method : create_volume                                                                       #
    #         summary      : create volume on ETERNUS                                              #
    #         parameters   : volume                                                                #
    #         return-value : volume meta data                                                      #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @lockutils.synchronized('ETERNUS_DX-vol', 'cinder-', True)
    def create_volume(self, volume):
        '''
        Create volume
        '''
        LOG.debug(_('*****create_volume,Enter method'))
        processing_time  = 0
        enter_time       = 0
        exit_time        = 0

        enter_time = time.time()

        metadata = self.common.create_volume(volume)

        exit_time = time.time()
        processing_time = exit_time-enter_time
        processing_time = str(processing_time)
        LOG.debug(_('*****create_volume,'
                    'processing time:%s sec') % processing_time)

        return {'metadata': metadata}

    #----------------------------------------------------------------------------------------------#
    # Method : create_volume_from_snapshot                                                         #
    #         summary      : create volume from snapshot                                           #
    #         parameters   : volume, snapshot                                                      #
    #         return-value : volume metadata                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @lockutils.synchronized('ETERNUS_DX-vol', 'cinder-', True)
    def create_volume_from_snapshot(self, volume, snapshot):
        '''
        Creates a volume from a snapshot.
        '''
        LOG.debug(_('*****creaet_volume_from_snapshot,Enter method'))
        processing_time  = 0
        enter_time       = 0
        exit_time        = 0
        enter_time = time.time()

        metadata = self.common.create_volume_from_snapshot(volume, snapshot)

        exit_time = time.time()
        processing_time = exit_time-enter_time
        processing_time = str(processing_time)
        LOG.debug(_('*****create_volume_from_snapshot,'
                    'processing time:%s sec') % processing_time)

        return {'metadata': metadata}

    #----------------------------------------------------------------------------------------------#
    # Method : create_cloned_volume                                                                #
    #         summary      : create cloned volume on ETERNUS                                       #
    #         parameters   : volume, src_vref                                                      #
    #         return-value : none                                                                  #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#

    def create_cloned_volume(self, volume, src_vref):
        """Creates a cloned volume."""
        self.common.create_cloned_volume(volume, src_vref)
        return

    #----------------------------------------------------------------------------------------------#
    # Method : delete_volume                                                                       #
    #         summary      : delete volume on ETERNUS                                              #
    #         parameters   : volume                                                                #
    #         return-value : none                                                                  #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @lockutils.synchronized('ETERNUS_DX-vol', 'cinder-', True)
    def delete_volume(self, volume):
        '''
        Delete volume on ETERNUS.
        '''
        LOG.debug(_('*****delete_volume,Enter method'))
        processing_time  = 0
        enter_time       = 0
        exit_time        = 0
        enter_time = time.time()

        self.common.delete_volume(volume)

        exit_time = time.time()
        processing_time = exit_time-enter_time
        processing_time = str(processing_time)
        LOG.debug(_('*****delete_volume,'
                    'processing time:%s sec') % processing_time)

        return

    #----------------------------------------------------------------------------------------------#
    # Method : create_snapshot                                                                     #
    #         summary      : create snapshot using SnapOPC                                         #
    #         parameters   : snapshot                                                              #
    #         return-value : none                                                                  #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @lockutils.synchronized('ETERNUS_DX-snap', 'cinder-', True)
    def create_snapshot(self, snapshot):
        '''
        Creates a snapshot.
        '''
        LOG.debug(_('*****create_snapshot,Enter method'))
        processing_time  = 0
        enter_time       = 0
        exit_time        = 0
        enter_time = time.time()

        self.common.create_snapshot(snapshot)

        exit_time = time.time()
        processing_time = exit_time-enter_time
        processing_time = str(processing_time)
        LOG.debug(_('*****create_snapshot,'
                    'processing time:%s sec') % processing_time)

        return

    #----------------------------------------------------------------------------------------------#
    # Method : delete_snapshot                                                                     #
    #         summary      : delete snapshot                                                       #
    #         parameters   : snapshot                                                              #
    #         return-value : none                                                                  #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @lockutils.synchronized('ETERNUS_DX-snap', 'cinder-', True)
    def delete_snapshot(self, snapshot):
        '''
        Deletes a snapshot.
        '''
        LOG.debug(_('*****delete_snapshot,Enter method'))
        processing_time  = 0
        enter_time       = 0
        exit_time        = 0
        enter_time = time.time()

        self.common.delete_snapshot(snapshot)

        exit_time = time.time()
        processing_time = exit_time-enter_time
        processing_time = str(processing_time)
        LOG.debug(_('*****delete_snapshot,'
                    'processing time:%s sec') % processing_time)

        return

    #----------------------------------------------------------------------------------------------#
    # Method : ensure_export                                                                       #
    #         summary      : Driver entry point to get the export info for an existing volume.     #
    #         parameters   : context, volume                                                       #
    #         return-value : none                                                                  #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#

    def ensure_export(self, context, volume):
        """Driver entry point to get the export info for an existing volume."""
        pass
        return

    #----------------------------------------------------------------------------------------------#
    # Method : create_export                                                                       #
    #         summary      : Driver entry point to get the export info for a new volume.           #
    #         parameters   : context, volume                                                       #
    #         return-value : none                                                                  #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#

    def create_export(self, context, volume):
        """Driver entry point to get the export info for a new volume."""
        pass
        return

    #----------------------------------------------------------------------------------------------#
    # Method : remove_export                                                                       #
    #         summary      : Driver entry point to remove an export for a volume.                  #
    #         parameters   : context, volume                                                       #
    #         return-value : none                                                                  #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#

    def remove_export(self, context, volume):
        """Driver entry point to remove an export for a volume."""
        pass
        return

    #----------------------------------------------------------------------------------------------#
    # Method : initialize_connection                                                               #
    #         summary      : set HostAffinityGroup on ETERNUS                                      #
    #         parameters   : volume, connector                                                     #
    #         return-value : connection info                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @lockutils.synchronized('ETERNUS_DX-attach', 'cinder-', True)
    def initialize_connection(self, volume, connector):
        '''
        Allow connection to connector and return connection info.
        '''
        LOG.debug(_('*****initialize_connection,Enter method'))
        processing_time  = 0
        enter_time       = 0
        exit_time        = 0
        enter_time = time.time()

        info = self.common.initialize_connection(volume, connector)

        exit_time = time.time()
        processing_time = exit_time-enter_time
        processing_time = str(processing_time)
        LOG.debug(_('*****initialize_connection,'
                    'processing time:%s sec') % processing_time)

        return info

    #----------------------------------------------------------------------------------------------#
    # Method : terminate_connection                                                                #
    #         summary      : remove HostAffinityGroup on ETERNUS                                   #
    #         parameters   : volume, connector                                                     #
    #         return-value :                                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    @lockutils.synchronized('ETERNUS_DX-attach', 'cinder-', True)
    def terminate_connection(self, volume, connector, **kwargs):
        '''
        Disallow connection from connector.
        '''
        LOG.debug(_('*****terminate_connection,Enter method'))
        processing_time  = 0
        enter_time       = 0
        exit_time        = 0
        enter_time = time.time()

        self.common.terminate_connection(volume, connector)

        exit_time = time.time()
        processing_time = exit_time-enter_time
        processing_time = str(processing_time)
        LOG.debug(_('*****terminate_connection,'
                    'processing time:%s sec') % processing_time)

        return

    #----------------------------------------------------------------------------------------------#
    # Method : get_volume_stats                                                                    #
    #         summary      : get pool capacity                                                     #
    #         parameters   : refresh                                                               #
    #         return-value : self.stats                                                            #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def get_volume_stats(self, refresh=False):
        '''
        Get volume stats.
        If 'refresh' is True, run update the stats first.
        '''
        LOG.debug(_('*****get_volume_stats,Enter method'))
        processing_time  = 0
        enter_time       = 0
        exit_time        = 0
        enter_time = time.time()

        if refresh == True:
            data                        = self.common.refresh_volume_stats()
            backend_name                = self.configuration.safe_get('volume_backend_name')
            data['volume_backend_name'] = backend_name or 'FJDXFCDriver'
            data['storage_protocol']    = 'fibre_channel'
            self._stats                 = data
        # end of if

        exit_time = time.time()
        processing_time = exit_time-enter_time
        processing_time = str(processing_time)
        LOG.debug(_('*****get_volume_stats,'
                    'processing time:%s sec') % processing_time)

        return self._stats

    #----------------------------------------------------------------------------------------------#
    # Method : copy_volume_to_image                                                                #
    #         summary      :                                                                       #
    #         parameters   :                                                                       #
    #         return-value :                                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        raise NotImplementedError()
        return

    #----------------------------------------------------------------------------------------------#
    # Method : copy_image_to_volume                                                                #
    #         summary      :                                                                       #
    #         parameters   :                                                                       #
    #         return-value :                                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def copy_image_to_volume(self, context, volume, image_service, image_id):
        raise NotImplementedError()
        return

