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
Cinder Volume driver for Fujitsu ETERNUS DX S2 and S3 series.
'''

##------------------------------------------------------------------------------------------------##
##                                                                                                ##
##  ETERNUS OpenStack Volume Driver                                                               ##
##                                                                                                ##
##  Note      :                                                                                   ##
##  File Name : fujitsu_eternus_dx_common.py                                                      ##
##  Copyright 2014 FUJITSU LIMITED                                                                ##
##                                                                                                ##
##  history :                                                                                     ##
##      2014.03 : 1.0.0 : volume(create,delete,attach,detach,create from snapshot)                ##
##                        snapshot(create,delete)                                                 ##
##      2014.04 : 1.0.1 : Fix comment                                                             ##
##------------------------------------------------------------------------------------------------##


import time
import hashlib
import base64
from oslo.config import cfg
from xml.dom.minidom import *
from xml.etree.ElementTree import *
from cinder import exception
from cinder.openstack.common import lockutils
from cinder.openstack.common import log as logging

LOG = logging.getLogger(__name__)

try:
    import pywbem
except:
    LOG.error(_('import pywbem failed!!'
               'pywbem is necessary for this volume driver.'))

#**************************************************************************************************#
CONF                   = cfg.CONF
VOL_PREFIX             = "FJosv_"
RAIDGROUP              = 2
TPPOOL                 = 5
SNAPOPC                = 4
OPC                    = 5
RETURN_TO_RESOURCEPOOL = 19
DETACH                 = 8
INITIALIZED            = 2
UNSYNCHRONIZED         = 3
BROKEN                 = 5
PREPARED               = 11
REPL                   = "FUJITSU_ReplicationService"
STOR_CONF              = "FUJITSU_StorageConfigurationService"
CTRL_CONF              = "FUJITSU_ControllerConfigurationService"
STOR_HWID              = "FUJITSU_StorageHardwareIDManagementService"

#**************************************************************************************************#
FJ_ETERNUS_DX_OPT_list = [cfg.StrOpt('cinder_eternus_config_file',
                                default='/etc/cinder/cinder_fujitsu_eternus_dx.xml',
                                help='config file for cinder fujitsu_eternus_dx volume driver')]

#**************************************************************************************************#
POOL_TYPE_dic          = {RAIDGROUP:'RAID_GROUP',
                          TPPOOL   :'Thinporvisioning_POOL'
                          }

OPERATION_dic          = {SNAPOPC:RETURN_TO_RESOURCEPOOL,
                          OPC    :DETACH
                          }

RETCODE_dic            = {'0'    :'Success',
                          '1'    :'Method Not Supported',
                          '4'    :'Failed',
                          '5'    :'Invalid Parameter',
                          '4097' :'Size Not Supported',
                          '32769':'Maximum number of Logical Volume in'
                                  ' a RAID group has been reached',
                          '32770':'Maximum number of Logical Volume in'
                                  ' the storage device has been reached',
                          '32785':'The RAID group is in busy state',
                          '32786':'The Logical Volume is in busy state',
                          '32787':'The device is in busy state',
                          '32788':'Element Name is in use',
                          '32792':'No Copy License',
                          '32796':'Quick Format Error',
                          '32801':'The CA port is in invalid setting',
                          '32802':'The Logical Volume is Mainframe volume',
                          '32803':'The RAID group is not operative',
                          '32804':'The Logical Volume is not operative',
                          '32808':'No Thin Provisioning License',
                          '32809':'The Logical Element is ODX volume',
                          '32816':'Fatal error generic',
                          '35318':'Maximum number of multi-hop has been reached',
                          '35331':'Maximum number of session has been reached(per device)',
                          '35333':'Maximum number of session has been reached(per SourceElement)',
                          '35334':'Maximum number of session has been reached(per TargetElement)',
                          '35346':'Copy table size is not setup'
                          }

#**************************************************************************************************#

#--------------------------------------------------------------------------------------------------#
# Class : FJDXCommon                                                                               #
#         summary      : cinder volume driver for Fujitsu ETERNUS DX                               #
#--------------------------------------------------------------------------------------------------#
class FJDXCommon(object):
    '''
    Common code that does not depend on protocol.
    '''

    #initialize
    stats = {'driver_version': '1.0',
             'free_capacity_gb': 0,
             'reserved_percentage': 0,
             'storage_protocol': None,
             'total_capacity_gb': 0,
             'vendor_name': 'FUJITSU',
             'volume_backend_name': None}

    #----------------------------------------------------------------------------------------------#
    # Method : __init__                                                                            #
    #         summary      :                                                                       #
    #         parameters   :                                                                       #
    #         return-value :                                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def __init__(self, prtcl, configuration=None):
        '''
        Constructor
        '''
        self.protocol      = prtcl
        self.configuration = configuration
        self.configuration.append_config_values(FJ_ETERNUS_DX_OPT_list)

        if prtcl == 'iSCSI':
            #get iSCSI ipaddress from driver configuration file
            self.configuration.iscsi_ip_address = self._get_drvcfg('EternusISCSIIP')
        # end of if
        self.conn = self._get_eternus_connection()
        return

    #----------------------------------------------------------------------------------------------#
    # Method : create_volume                                                                       #
    #         summary      : create volume on ETERNUS                                              #
    #         parameters   : volume                                                                #
    #         return-value : volume meta data                                                      #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def create_volume(self, volume):
        '''
        create volume on ETERNUS
        '''
        #volumesize   : volume size Byte
        #volumename   : volumename on ETERNUS
        #eternus_pool : poolname
        #pool         : pool instance
        #pooltype     : RAID(2) or TPP(5)
        #configservice: FUJITSU_StorageConfigurationService
        #msg          : message
        #rc           : result of invoke method
        #errordesc    : error message
        #job          : unused

        LOG.debug(_('*****create_volume,Enter method'))

        #initialize
        systemnamelist = None
        volumesize     = 0
        volumename     = None
        eternus_pool   = None
        pool           = None
        pooltype       = 0
        configservice  = None
        msg            = None
        rc             = 0
        errordesc      = None
        job            = None

        #main processing
        #conversion of the unit. GB to B
        volumesize = int(volume['size']) * 1073741824
        #create to volumename on ETERNUS from cinder VolumeID
        volumename = self._create_volume_name(volume['id'])
        LOG.debug(_('*****create_volume,volumename:%(volumename)s,'
                    'volumesize:%(volumesize)u')
                   % {'volumename': volumename,
                      'volumesize': volumesize})

        self.conn = self._get_eternus_connection()

        #get poolname from driver configuration file
        eternus_pool = self._get_drvcfg('EternusPool')
        LOG.debug(_('*****create_volume,'
                    'eternus_pool:%(eternus_pool)s')
                   % {'eternus_pool': eternus_pool})

        #find storage pool
        pool = self._find_pool(eternus_pool)
        if pool is None:
            msg = (_('create_volume,'
                     'eternus_pool:%(eternus_pool)s,'
                     'not found.')
                    % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        if 'RSP' in pool['InstanceID']:
            pooltype = RAIDGROUP
        else:
            pooltype = TPPOOL
        # end of if
        configservice = self._find_eternus_service(STOR_CONF)
        if configservice is None:
            msg = (_('create_volume,volume:%(volume)s,'
                     'volumename:%(volumename)s,'
                     'eternus_pool:%(eternus_pool),'
                     'Error!! Storage Configuration Service is None.')
                    % {'volume':volume,
                       'volumename': volumename,
                       'eternus_pool':eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if
        LOG.debug(_('*****create_volume,CreateOrModifyElementFromStoragePool,'
                    'ConfigService:%(service)s,'
                    'ElementName:%(volumename)s,'
                    'InPool:%(eternus_pool)s,'
                    'ElementType:%(pooltype)u,'
                    'Size:%(volumesize)u')
                   % {'service':configservice,
                      'volumename': volumename,
                      'eternus_pool':eternus_pool,
                      'pooltype':pooltype,
                      'volumesize': volumesize})

        #create volume on ETERNUS
        rc, errordesc, job = self._exec_eternus_service(
            'CreateOrModifyElementFromStoragePool',
            configservice,
            ElementName=volumename,
            InPool=pool,
            ElementType=pywbem.Uint16(pooltype),
            Size=pywbem.Uint64(volumesize))

        if rc != 0L:
            msg = (_('create_volume,'
                     'volumename:%(volumename)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s')
                    % {'volumename': volumename,
                       'rc': rc,
                       'errordesc':errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        try:
            systemnamelist = self.conn.EnumerateInstances(
                'FUJITSU_StorageProduct')
        except:
            msg=(_('create_volume,'
                   'volume:%(volume)s,'
                   'EnumerateInstances,'
                   'cannot connect to ETERNUS.')
                  % {'volume':volume})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug(_('*****create_volume,'
                    'volumename:%(volumename)s,'
                    'Return code:%(rc)lu,'
                    'Error:%(errordesc)s,'
                    'Backend:%(backend)s,'
                    'Pool Name:%(eternus_pool)s,'
                    'Pool Type:%(pooltype)s,'
                    'Leaving create_volume')
                   % {'volumename': volumename,
                      'rc': rc,
                      'errordesc':errordesc,
                      'backend':systemnamelist[0]['IdentifyingNumber'],
                      'eternus_pool':eternus_pool,
                      'pooltype':POOL_TYPE_dic[pooltype]})

        return {'Backend':systemnamelist[0]['IdentifyingNumber'],
                'Volume Name':volumename,
                'Pool Name':eternus_pool,
                'Pool Type':POOL_TYPE_dic[pooltype]}


    #----------------------------------------------------------------------------------------------#
    # Method : create_volume_from_snapshot                                                         #
    #         summary      : create volume from snapshot                                           #
    #         parameters   : volume, snapshot                                                      #
    #         return-value : volume metadata                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def create_volume_from_snapshot(self, volume, snapshot):
        '''
        Creates a volume from a snapshot
        '''
        #sysnames              : FUJITSU_SrogareProduct
        #backend               : ETERNUS model
        #snapshotname          : snapshotname on OpenStack
        #t_volumename          : target volumename on ETERNUS
        #s_volumename          : source volumename on ETERNUS
        #eternus_pool          : poolname
        #pool                  : pool instance
        #pooltype              : RAID(2) or TPP(5)
        #configservice         : FUJITSU_StorageConfigurationService
        #source_volume_instance: snapshot instance
        #msg                   : message
        #rc                    : result of invoke method
        #errordesc             : error message
        #job                   : unused

        LOG.debug(_('*****create_volume_from_snapshot,Enter method'))

        #initialize
        systemnamelist           = None
        systemname               = None
        snapshotname             = None
        t_volumename             = None
        s_volumename             = None
        eternus_pool             = None
        pool                     = None
        pooltype                 = 0
        configservice            = None
        source_volume_instance   = None
        msg                      = None
        rc                       = 0
        errordesc                = None
        job                      = None

        #main processing
        snapshotname           = snapshot['name']
        t_volumename           = self._create_volume_name(volume['id'])
        s_volumename           = self._create_volume_name(snapshot['id'])
        self.conn              = self._get_eternus_connection()
        source_volume_instance = self._find_lun(s_volumename)

        if source_volume_instance is None:
            msg=(_('create_volume_from_snapshot,'
                   'Source Volume is not exist in ETERNUS.'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        try:
            systemnamelist = self.conn.EnumerateInstances(
                'FUJITSU_StorageProduct')
        except:
            msg=(_('create_volume_from_snapshot,'
                   'volume:%(volume)s,'
                   'EnumerateInstances,'
                   'cannot connect to ETERNUS.')
                  % {'volume':volume})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        systemname = systemnamelist[0]['IdentifyingNumber']

        LOG.debug(_('*****create_volume_from_snapshot,'
                    'volumename:%(volumename)s,'
                    'snapshotname:%(snapshotname)s,'
                    'source volume instance:%(source_volume_instance)s,')
                   % {'volumename': t_volumename,
                      'snapshotname': snapshotname,
                      'source_volume_instance': str(source_volume_instance.path)})

        #get configservice for CreateReplica
        configservice = self._find_eternus_service(STOR_CONF)

        if configservice is None:
            msg = (_('create_volume_from_snapshot,'
                     'Storage Configuration Service not found'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        eternus_pool = self._get_drvcfg('EternusPool')
        pool = self._find_pool(eternus_pool)
        if pool is None:
            msg = (_('create_volume_from_snapshot,'
                     'eternus_pool:%(eternus_pool)s,'
                     'not found.')
                    % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        if 'RSP' in pool['InstanceID']:
            pooltype = RAIDGROUP
        else:
            pooltype = TPPOOL
        # end of if

        # Create a Clone from snapshot
        rc, errordesc, job = self._exec_eternus_service(
            'CreateReplica',
            configservice,
            ElementName=t_volumename,
            TargetPool=pool,
            CopyType=pywbem.Uint16(5),
            SourceElement=source_volume_instance.path)

        if rc != 0L:
            msg = (_('create_volume_from_snapshot,'
                     'volumename:%(volumename)s,'
                     'snapshotname:%(snapshotname)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s')
                    % {'volumename': t_volumename,
                       'snapshotname': snapshotname,
                       'rc': rc,
                       'errordesc':errordesc})
            LOG.error(msg)
            if rc == 5 and str(systemname[4]) == '2':
                msg = (_('create_volume_from_snapshot,'
                         'NOT supported on DX S2[%(backend)s].')
                        % {'backend':systemname})
                LOG.error(msg)
            # end of if
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****create_volume_from_snapshot,Exit method'))

        #systemname = systemnamelist[0]['IdentifyingNumber']
        # ex) ET092DC4511133A10
        return {'Backend':systemname,
                'Volume Name':t_volumename,
                'Pool Name':eternus_pool,
                'Pool Type':POOL_TYPE_dic[pooltype]}


    #----------------------------------------------------------------------------------------------#
    # Method : create_cloned_volume                                                                #
    #         summary      : create cloned volume on ETERNUS                                       #
    #         parameters   : volume, src_vref                                                      #
    #         return-value : none                                                                  #
    #         exceptions   : NotImplementedError                                                   #
    #----------------------------------------------------------------------------------------------#
    def create_cloned_volume(self, volume, src_vref):
        '''
        Creates a clone of the specified volume.
        '''
        raise NotImplementedError()
        return

    #----------------------------------------------------------------------------------------------#
    # Method : delete_volume                                                                       #
    #         summary      : delete volume on ETERNUS                                              #
    #         parameters   : volume                                                                #
    #         return-value : none                                                                  #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def delete_volume(self, volume):
        '''
        Delete volume on ETERNUS.
        '''
        #volumename   : volumename on ETERNUS
        #vol_instance : volume instance
        #cps          : copy session instance
        #configservice: FUJITSU_StorageConfigurationService
        #msg          : message
        #rc           : result of invoke method
        #errordesc    : error message
        #job          : unused

        LOG.debug(_('*****delete_volume,Enter method'))

        #initialize
        volumename    = None
        vol_instance  = None
        cpsession     = None
        configservice = None
        msg           = None
        rc            = 0
        errordesc     = None
        job           = None

        #main preprocessing
        volumename = self._create_volume_name(volume['id'])
        self.conn  = self._get_eternus_connection()

        #if volume is used by copysession,
        #stop the copysession.
        cpsession = self._find_copysession(volumename)
        if cpsession is not None:
            LOG.debug(_('*****delete_volume,volumename:%(volumename)s,'
                        'volume is using by copysession[%(cpsession)s].delete copysession.')
                       % {'volumename': volumename,
                         'cpsession': cpsession})
            self._delete_copysession(cpsession)
        # end of if

        #Existence confirmation
        vol_instance = self._find_lun(volumename)
        if vol_instance is None:
            LOG.debug(_('*****delete_volume,volumename:%(volumename)s,'
                        'volume not found on ETERNUS.'
                        'delete only management data on cinder database.')
                      % {'volumename': volumename})
            return
        # end of if

        #begin volume deletion processing
        configservice = self._find_eternus_service(STOR_CONF)
        if configservice is None:
            msg = (_('delete_volume,volumename:%(volumename)s,'
                     'Storage Configuration Service not found.')
                    % {'volumename': volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****delete_volume,volumename:%(volumename)s,'
                    'vol_instance:%(vol_instance)s,'
                    'Method: ReturnToStoragePool')
                   % {'volumename': volumename,
                      'vol_instance': str(vol_instance.path)})

        #Invoke method for delete volume
        rc, errordesc, job = self._exec_eternus_service(
            'ReturnToStoragePool',
            configservice,
            TheElement=vol_instance.path)

        #when a method did Fail
        if rc != 0L:
            msg = (_('delete_volume,volumename:%(volumename)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s')
                    % {'volumename': volumename,
                       'rc': rc,
                       'errordesc': errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****delete_volume,volumename:%(volumename)s,'
                    'Return code:%(rc)lu,'
                    'Error:%(errordesc)s,'
                    'Exit Method')
                  % {'volumename': volumename,
                     'rc': rc,
                     'errordesc': errordesc})

        return

    #----------------------------------------------------------------------------------------------#
    # Method : create_snapshot                                                                     #
    #         summary      : create snapshot using SnapOPC                                         #
    #         parameters   : snapshot                                                              #
    #         return-value : none                                                                  #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def create_snapshot(self, snapshot):
        '''
        create snapshot using SnapOPC
        '''
        #snapshotname  : snapshot name on Openstack
        #volumename    : source volume name on Openstack
        #vol_id        : source volume id
        #volume        : source volume dictionary
        #vol_instance  : source volume instance
        #s_volumename  : source volume name on ETERNUS
        #d_volumename  : destination volume name on ETERNUS
        #eternus_pool  : poolname
        #pool          : pool instance
        #configservice : FUJITSU_StorageConfigurationService
        #msg           : message
        #rc            : result of invoke method
        #errordesc     : error message
        #job           : unused

        LOG.debug(_('*****create_snapshot,Enter method'))

        #initialize
        snapshotname  = None
        volumename    = None
        vol_id        = None
        volume        = None
        vol_instance  = None
        d_volumename  = None
        s_volumename  = None
        vol_instance  = None
        configservice = None
        eternus_pool  = None
        pool          = None
        msg           = None
        rc            = 0
        errordesc     = None
        job           = None

        #main processing
        snapshotname  = snapshot['name']
        volumename    = snapshot['volume_name']
        vol_id        = snapshot['volume_id']
        volume        = snapshot['volume']
        d_volumename  = self._create_volume_name(snapshot['id'])
        s_volumename  = self._create_volume_name(vol_id)
        vol_instance  = self._find_lun(s_volumename)
        configservice = self._find_eternus_service(STOR_CONF)

        if vol_instance is None:
            # source volume is not found on ETERNUS
            msg = (_('create_snapshot,'
                     'volumename on ETERNUS:%(s_volumename)s,'
                     'source volume is not found on ETERNUS.')
                    % {'s_volumename': s_volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        if configservice is None:
            msg = (_('create_snapshot,'
                     'volumename:%(volumename)s,'
                     'Storage Configuration Service not found.')
                    % {'volumename': volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        eternus_pool  = self._get_drvcfg('EternusPool')
        self.conn     = self._get_eternus_connection()
        pool          = self._find_pool(eternus_pool)
        if pool is None:
            msg = (_('create_snapshot,'
                     'eternus_pool:%(eternus_pool)s,'
                     'not found.')
                    % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****create_snapshot,'
                    'snapshotname:%(snapshotname)s,'
                    'source volume name:%(volumename)s,'
                    'vol_instance.path:%(vol_instance)s,'
                    'dest_volumename:%(d_volumename)s,'
                    'pool:%(pool)s,'
                    'Invoke CreateReplica')
                   % {'snapshotname': snapshotname,
                      'volumename': volumename,
                      'vol_instance': str(vol_instance.path),
                      'd_volumename': d_volumename,
                      'pool': pool})

        rc, errordesc, job = self._exec_eternus_service(
            'CreateReplica',
            configservice,
            ElementName=d_volumename,
            TargetPool=pool,
            CopyType=pywbem.Uint16(4),
            SourceElement=vol_instance.path)

        if rc != 0L:
            msg = (_('create_snapshot,'
                     'snapshotname:%(snapshotname)s,'
                     'source volume name:%(volumename)s,'
                     'vol_instance.path:%(vol_instance)s,'
                     'dest_volumename:%(d_volumename)s,'
                     'pool:%(pool)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s')
                    % {'snapshotname': snapshotname,
                       'volumename': volumename,
                       'vol_instance': str(vol_instance.path),
                       'd_volumename': d_volumename,
                       'pool': pool,
                       'rc': rc,
                       'errordesc':errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****create_snapshot,volumename:%(volumename)s,'
                    'Return code:%(rc)lu,'
                    'Error:%(errordesc)s,'
                    'Exit Method')
                  % {'volumename': volumename,
                     'rc': rc,
                     'errordesc': errordesc})

        return

    #----------------------------------------------------------------------------------------------#
    # Method : delete_snapshot                                                                     #
    #         summary      : delete snapshot                                                       #
    #         parameters   : snapshot                                                              #
    #         return-value : none                                                                  #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def delete_snapshot(self, snapshot):
        '''
        delete snapshot
        '''
        # Delete snapshot - deletes the target element and the snap session
        #snapshotname  : snapshot display name
        #volumename    : source volume display name on openstack
        #d_volumename  : destination volume name on ETERNUS
        #snapshot['id']: destination volume id for _find_lun
        #cpsession     : Storage Synchronized
        #repservice    : FUJITSU_ReplicationService
        #msg           : message
        #rc            : result of invoke method
        #errordesc     : error message

        LOG.debug(_('*****delete_snapshot,Enter method'))

        #initialize

        #main processing

        self.delete_volume(snapshot)

        LOG.debug(_('*****delete_snapshot,Exit method'))
        return

    #----------------------------------------------------------------------------------------------#
    # Method : initialize_connection                                                               #
    #         summary      : set HostAffinityGroup on ETERNUS                                      #
    #         parameters   : volume, connector                                                     #
    #         return-value : connection info                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def initialize_connection(self, volume, connector):
        '''
        Allow connection to connector and return connection info.
        '''
        #targetlist : target port id list
        #data       : the element which constitutes device_info
        #msg        : message
        #device_info: return value

        LOG.debug(_('*****initialize_connection,Enter method'))

        #initialize
        targetlist  = []
        data        = {}
        msg         = None
        device_info = {}

        #main processing
        self.conn  = self._get_eternus_connection()
        targetlist = self._get_target_portid(connector)
        data       = self._find_device_number(volume, connector, targetlist)

        if len(data) == 0:
            # volume not found on ETERNUS
            msg = (_('initialize_connection,'
                     'volume:%(volume)s,'
                     'Volume not found.')
                    % {'volume':volume['name']})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if data['target_lun'] is not None:
            # volume is already mapped
            msg = (_('initialize_connection,'
                     'volume:%(volume)s,'
                     'target_lun:%(target_lun)s,'
                     'Volume is already mapped.')
                    % {'volume':volume['name'],
                       'target_lun':data['target_lun']})
            LOG.info(msg)
        else:
            self._map_lun(volume, connector, targetlist)
            data = self._find_device_number(volume, connector, targetlist)
        # end of if

        data['target_discoverd'] = True
        data['volume_id']        = volume['id']

        if self.protocol == 'iSCSI':
            device_info = {'driver_volume_type': 'iscsi',
                           'data': data}
        elif self.protocol == 'fc':
            device_info = {'driver_volume_type': 'fibre_channel',
                           'data': data}
        # end of if

        LOG.debug(_('*****initialize_connection,'
                    'device_info:%(info)s,'
                    'Exit method')
                  % {'info': device_info})

        return device_info


    #----------------------------------------------------------------------------------------------#
    # Method : terminate_connection                                                                #
    #         summary      : remove HostAffinityGroup on ETERNUS                                   #
    #         parameters   : volume, connector                                                     #
    #         return-value :                                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def terminate_connection(self, volume, connector, force=False, **kwargs):
        '''
        Disallow connection from connector
        '''
        LOG.debug(_('*****terminate_connection,Enter method'))

        #main processing
        self.conn = self._get_eternus_connection()
        self._unmap_lun(volume, connector)

        LOG.debug(_('*****terminate_connection,Exit method'))
        return



    #----------------------------------------------------------------------------------------------#
    # Method : refresh_volume_stats                                                                #
    #         summary      : get pool capacity                                                     #
    #         parameters   : None                                                                  #
    #         return-value : self.stats                                                            #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def refresh_volume_stats(self):
        '''
        get pool capacity.
        '''
        #eternus_pool : poolname
        #pool         : pool instance

        LOG.debug(_('*****refresh_volume_stats,Enter method'))

        #initialize
        eternus_pool                    = None
        pool                            = None

        #main processing
        self.conn                       = self._get_eternus_connection()
        eternus_pool                    = self._get_drvcfg('EternusPool')
        pool                            = self._find_pool(eternus_pool,True)
        if pool is None:
            msg = (_('refresh_volume_stats,'
                     'eternus_pool:%(eternus_pool)s,'
                     'not found.')
                    % {'eternus_pool': eternus_pool})
            LOG.info(msg)
            self._create_pool(eternus_pool)
            pool = self._find_pool(eternus_pool,True)
            if pool is None:
                msg = (_('refresh_volume_stats,'
                         'eternus_pool:%(eternus_pool)s,'
                         'not found after create_pool.')
                        % {'eternus_pool': eternus_pool})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if
        # end of if

        self.stats['total_capacity_gb'] = pool['TotalManagedSpace']
        self.stats['free_capacity_gb']  = pool['RemainingManagedSpace']

        LOG.debug(_('*****refresh_volume_stats,'
                    'eternus_pool:%(eternus_pool)s,'
                    'total capacity[%(total)s],'
                    'free capacity[%(free)s]')
                   % {'eternus_pool':eternus_pool,
                      'total':self.stats['total_capacity_gb'],
                      'free':self.stats['free_capacity_gb']})

        return self.stats

    #----------------------------------------------------------------------------------------------#
    # Method : _find_device_number                                                                 #
    #         summary      : return number of mapping order                                        #
    #         parameters   : volume, connector, targetlist                                         #
    #         return-value : mapping order                                                         #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _find_device_number(self, volume, connector, targetlist = []):
        '''
        return mapping order
        '''
        #map_num        : number of mapping order
        #map_num_tmp    : map_num temporary
        #data           : device information
        #volumename     : volumename on ETERNUS
        #vol_instance   : volume instance
        #targetlist     : target portid
        #volmaplist     : volume mapping information list
        #volmap         : volume mapping information
        #volmapinstance : volume mapping instance
        #iqn            : iSCSI Qualified Name

        LOG.debug(_('*****_find_device_number,Enter method'))

        #initialize
        map_num        = None
        map_num_tmp    = None
        data           = {}
        volumename     = None
        vol_instance   = None
        volmaplist     = []
        volmap         = None
        volmapinstance = {}
        iqn            = None

        #main processing
        volumename   = self._create_volume_name(volume['id'])
        vol_instance = self._find_lun(volumename)

        if vol_instance is not None:
            #get volume mapping order
            try:
                volmaplist = self.conn.ReferenceNames(
                    vol_instance.path,
                    ResultClass='CIM_ProtocolControllerForUnit')
            except:
                msg=(_('_find_device_number,'
                       'volume:%(volume)s,'
                       'ReferenceNames,'
                       'cannot connect to ETERNUS.')
                      % {'volume':volume})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug(_('*****_find_device_number,'
                        'volmaplist:%(volmaplist)s')
                       % {'volmaplist':volmaplist})

            if len(volmaplist) != 0:
                try:
                    volmapinstance = self.conn.GetInstance(
                        volmaplist[0],
                        LocalOnly=False)
                except:
                    msg=(_('_find_device_number,'
                           'volume:%(volume)s,'
                           'GetInstance,'
                           'cannot connect to ETERNUS.')
                          % {'volume':volume})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

                map_num = int(volmapinstance['DeviceNumber'], 16)

                # compare value after 2nd with 1st value.
                for volmap in volmaplist[1:]:
                    try:
                        volmapinstance = self.conn.GetInstance(
                            volmap,
                            LocalOnly=False)
                    except:
                        msg=(_('_find_device_number,'
                               'volume:%(volume)s,'
                               'GetInstance,'
                               'cannot connect to ETERNUS.')
                             % {'volume':volume})
                        LOG.error(msg)
                        raise exception.VolumeBackendAPIException(data=msg)

                    map_num_tmp = int(volmapinstance['DeviceNumber'], 16)
                    if map_num != map_num_tmp:
                        msg = (_('_find_device_number,'
                                 'Device number of each AffinityGroup does not accord.'
                                 'map_num:%(map_num)d,'
                                 'map_num_tmp:%(map_num_tmp)d')
                                % {'map_num': map_num,
                                   'map_num_tmp': map_num_tmp})
                        LOG.info(msg)
                    # end of if
                    LOG.debug(_('*****_find_device_number,'
                                'map_num:%(map_num)d,'
                                'map_num_tmp:%(map_num_tmp)d')
                               % {'map_num': map_num,
                                  'map_num_tmp': map_num_tmp})
                # end of for volmaplist
            # end of if

            if map_num is None:
                msg = (_('_find_device_number,'
                         'Device number not found for volume'
                         '%(volumename)s %(vol_instance)s.')
                        % {'volumename': volumename,
                           'vol_instance': str(vol_instance.path)})
                LOG.info(msg)

            else:
                LOG.debug(_('*****_find_device_number,'
                            'Found device number %(device)d for volume'
                            ' %(volumename)s %(vol_instance)s.')
                           % {'device': map_num,
                              'volumename': volumename,
                              'vol_instance': str(vol_instance.path)})
            # end of if

            if self.protocol == 'iSCSI':
                iqn = self._get_iqn()

                LOG.debug(_('*****_find_device_number,'
                            'iqn:%(iqn)s')
                           % {'iqn':iqn})
                target_portal = '%s:%s' % (self.configuration.iscsi_ip_address,
                                           self.configuration.iscsi_port)
                data = {'target_lun': map_num,
                        'target_iqn': iqn,
                        'target_portal': target_portal}

            elif self.protocol == 'fc':
                if len(targetlist) == 0:
                    targetlist = self._get_target_portid(connector)
                # end of if

                LOG.debug(_('*****_find_device_number,'
                            'targetlist:%(targetlist)s')
                           % {'targetlist':targetlist})

                data = {'target_lun': map_num,
                        'target_wwn': targetlist}
            # end of if

            LOG.debug(_('*****_find_device_number,Device info: %(data)s.')
                       % {'data': data})
        else:
            LOG.debug(_('*****_find_device_number,Device does not found.'))
        # end of if

        return data


    #----------------------------------------------------------------------------------------------#
    # Method : _get_devcfg                                                                         #
    #         summary      : read parameter from driver configuration file                         #
    #         parameters   : tagname, filename                                                     #
    #         return-value : value of tagname                                                      #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _get_drvcfg(self,tagname,filename=None):
        '''
        read from driver configuration file.
        '''
        #filename  : driver configuration file name
        #tagname   : xml tagname
        #tree      : element tree from driver configuration file
        #elem      : root element
        #ret       : return value

        LOG.debug(_('*****_get_drvcfg,Enter method'))

        #initialize
        tree = None
        elem = None
        ret  = None

        #main processing
        if filename is None:
            #set default configuration file name
            filename = self.configuration.cinder_eternus_config_file
        # end of if

        LOG.debug(_("*****_get_drvcfg input[%s][%s]") %(filename, tagname))

        tree = parse(filename)
        elem = tree.getroot()
        ret  = elem.findtext(".//"+tagname)

        if ret is None:
            msg = (_('_get_drvcfg,'
                     'filename:%(filename)s,'
                     'tagname:%(tagname)s,'
                     'data is None!!  '
                     'Please edit driver configuration file and correct an error. ')
                    % {'filename':filename,
                       'tagname':tagname})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_("*****_get_drvcfg output[%s]") %(ret))

        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _get_eternus_connection                                                             #
    #         summary      : return WBEM connection                                                #
    #         parameters   : filename                                                              #
    #         return-value : WBEM connection                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _get_eternus_connection(self, filename=None):
        '''
        return WBEM connection
        '''
        #filename  : driver configuration file name
        #ip        : SMI-S IP address
        #port      : SMI-S port
        #user      : SMI-S username
        #password  : SMI-S password
        #url       : SMI-S connection url
        #conn      : WBEM connection

        LOG.debug(_("*****_get_eternus_connection [%s],"
                    "Enter method")
                   % filename)

        #initialize
        ip       = None
        port     = None
        user     = None
        password = None
        url      = None
        conn     = None

        #main processing
        ip     = self._get_drvcfg('EternusIP', filename)
        port   = self._get_drvcfg('EternusPort', filename)
        user   = self._get_drvcfg('EternusUser', filename)
        passwd = self._get_drvcfg('EternusPassword', filename)
        url    = 'http://'+ip+':'+port

        conn   = pywbem.WBEMConnection(url, (user, passwd),
                                     default_namespace='root/eternus')

        if conn is None:
            msg = (_('_get_eternus_connection,'
                     'filename:%(filename)s,'
                     'ip:%(ip)s,'
                     'port:%(port)s,'
                     'user:%(user)s,'
                     'passwd:%(passwd)s,'
                     'url:%(url)s,'
                     'FAILED!!.')
                    % {'filename':filename,
                       'ip':ip,
                       'port':port,
                       'user':user,
                       'passwd':passwd,
                       'url':url})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****_get_eternus_connection,[%s],Exit method') % (conn))

        return conn

    #----------------------------------------------------------------------------------------------#
    # Method : _create_volume_name                                                                 #
    #         summary      : create volume_name on ETERNUS from id on OpenStack.                   #
    #         parameters   : id_code                                                               #
    #         return-value : volumename on ETERNUS                                                 #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _create_volume_name(self, id_code):
        '''
        create volume_name on ETERNUS from id on OpenStack.
        '''
        #id_code         : volume_id, snapshot_id etc..
        #m               : hashlib.md5 instance
        #ret             : volumename on ETERNUS
        #systemnamelist  : ETERNUS information list
        #systemname      : ETERNUS model information

        LOG.debug(_('*****_create_volume_name [%s],Enter method.')
                   % id_code)

        #initialize
        m               = None
        ret             = None
        systemnamelist  = None
        systemname      = None

        #main processing
        if id_code is None:
            msg=(_('_create_volume_name,'
                   'id_code is None.'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        m = hashlib.md5()
        m.update(id_code)
        ret = VOL_PREFIX + str(base64.urlsafe_b64encode(m.digest()))

        #get ETERNUS model
        try:
            systemnamelist = self.conn.EnumerateInstances(
                'FUJITSU_StorageProduct')
        except:
            msg=(_('create_volume_name,'
                   'id_code:%(id_code)s,'
                   'EnumerateInstances,'
                   'cannot connect to ETERNUS.')
                  % {'id_code':id_code})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        systemname = systemnamelist[0]['IdentifyingNumber']
        #systemname = systemnamelist[0]['IdentifyingNumber']
        # ex) ET092DC4511133A10
        LOG.debug(_('*****_create_volume_name,'
                    'systemname:%(systemname)s,'
                    'storage is DX S%(model)s')
                   % {'systemname':systemname,
                      'model':systemname[4]})

        #shorten volumename when storage is DX S2 series
        if str(systemname[4]) == '2':
            LOG.debug(_('*****_create_volume_name,'
                        'volumename is 16 digit.'))
            ret = ret[:16]
        # end of if

        LOG.debug(_('*****_create_volume_name,'
                    'ret:%(ret)s,'
                    'Exit method.')
                   % {'ret':ret})

        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _get_pool_instance_id                                                               #
    #         summary      : get pool instacne_id from pool name on ETERNUS                        #
    #         parameters   : eternus_pool                                                          #
    #         return-value : pool instance id                                                      #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _get_pool_instance_id(self, eternus_pool):
        '''
        get pool instacne_id from pool name on ETERNUS
        '''
        #eternus_pool  : pool name on ETERNUS.
        #poolinstanceid: ETERNUS pool instance id(return value)
        #tppoollist    : list of thinprovisioning pool on ETERNUS.
        #rgpoollist    : list of raid group on ETERNUS.
        #tppool        : Thinprovisioning Pool
        #rgpool        : RAID Group
        #msg           : message

        LOG.debug(_('*****_get_pool_instance_id,'
                    'Enter method'))

        #initialize
        poolinstanceid = None
        tppoollist    = []
        rgpoollist    = []
        tppool        = None
        rgpool        = None
        poolname      = None
        msg           = None


        #main processing
        try:
            rgpoollist = self.conn.EnumerateInstances(
                'FUJITSU_RAIDStoragePool')
            tppoollist = self.conn.EnumerateInstances(
                'FUJITSU_ThinProvisioningPool')
        except:
            msg=(_('_get_pool_instance_id,'
                   'eternus_pool:%(eternus_pool)s,'
                   'EnumerateInstances,'
                   'cannot connect to ETERNUS.')
                  % {'eternus_pool':eternus_pool})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)


        for rgpool in rgpoollist:
            poolname = rgpool['ElementName']
            if str(eternus_pool) == str(poolname):
                poolinstanceid = rgpool['InstanceID']
                break
            # end of if
        else:
            for tppool in tppoollist:
                poolname = tppool['ElementName']
                if str(eternus_pool) == str(poolname):
                    poolinstanceid = tppool['InstanceID']
                    break
                # end of if
            # end of for tppoollist
        # end of for rgpoollist

        if poolinstanceid is None:
            msg = (_('_get_pool_instance_id,'
                     'eternus_pool:%(eternus_pool)s,'
                     'poolinstanceid is None.')
                    % {'eternus_pool': eternus_pool})
            LOG.info(msg)
        # end of if

        LOG.debug(_('*****_get_pool_instance_id,'
                    'Exit method'))

        return poolinstanceid

    #----------------------------------------------------------------------------------------------#
    # Method : _create_pool                                                                        #
    #         summary      : create raidgroup on ETERNUS                                           #
    #         parameters   : eternus_pool                                                          #
    #         return-value : None                                                                  #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _create_pool(self, eternus_pool):
        '''
          create raidgroup on ETERNUS.
        '''
        #  create raidgroup on ETERNUS.
        #  raidgroup name is eternus_pool.
        #  raidlevel and diskdrives are automatically selected.
        #
        #eternus_pool : poolname on ETERNUS
        #configservice: FUJITSU_StorageConfigurationService
        #msg          : message
        #rc           : result of invoke method
        #errordesc    : error message
        #job          : unused

        LOG.debug(_('*****_create_pool,'
                    'eternus_pool:%(eternus_pool)s,'
                    'Enter method.')
                   % {'eternus_pool': eternus_pool})

        #initialize
        poolinstance  = None
        configservice = None
        msg           = None
        rc            = 0
        errordesc     = None
        job           = None

        #main processing
        configservice = self._find_eternus_service(STOR_CONF)
        if configservice is None:
            msg = (_('_create_pool,'
                     'eternus_pool:%(eternus_pool)s,'
                     'configservice is None.')
                    % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        #create pool on ETERNUS
        #Raid Level and DiskDrives are automatically selected.
        rc, errordesc, job = self._exec_eternus_service(
            'CreateOrModifyStoragePool',
            configservice,
            ElementName=eternus_pool)

        if rc != 0L:
            msg=(_('_create_pool,'
                   'eternus_pool:%(eternus_pool)s,'
                   'Return code:%(rc)lu,'
                   'Error:%(errordesc)s')
                  % {'eternus_pool':eternus_pool,
                     'rc':rc,
                     'errordesc':errordesc})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****_create_pool,'
                    'eternus_pool:%(eternus_pool)s,'
                    'Exit method.')
                   % {'eternus_pool': eternus_pool})

        return


    #----------------------------------------------------------------------------------------------#
    # Method : _find_pool                                                                          #
    #         summary      : find Instance or Name of pool by pool name on ETERNUS.                #
    #         parameters   : eternus_pool, detail                                                  #
    #         return-value : pool instance or instance name                                        #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _find_pool(self, eternus_pool, detail=False):
        '''
        find Instance or InstanceName of pool by pool name on ETERNUS.
        '''
        #eternus_pool  : poolname
        #detail        : False >return EnumerateInstanceNames
        #              : True  >return EnumerateInstances
        #poolinstanceid: pool instance id
        #poolinstance  : pool instance
        #msg           : message
        #tppoollist    : list of thinprovisioning pool on ETERNUS.
        #rgpoollist    : list of raid group on ETERNUS.

        LOG.debug(_('*****_find_pool,Enter method'))

        #initialize
        poolinstanceid = None
        poolinstance   = None
        msg            = None
        tppoollist     = []
        rgpoollist     = []

        #main processing
        poolinstanceid = self._get_pool_instance_id(eternus_pool)
        #if pool instance is None then create pool on ETERNUS.
        if poolinstanceid is None:
            msg = (_('_find_pool,'
                     'eternus_pool:%(eternus_pool)s,'
                     'poolinstanceid is None.')
                    % {'eternus_pool': eternus_pool})
            LOG.info(msg)

        else:

            if detail is True:
                try:
                    tppoollist = self.conn.EnumerateInstances(
                        'FUJITSU_ThinProvisioningPool')
                    rgpoollist = self.conn.EnumerateInstances(
                        'FUJITSU_RAIDStoragePool')
                except:
                    msg=(_('_find_pool,'
                           'eternus_pool:%(eternus_pool)s,'
                           'EnumerateInstances,'
                           'cannot connect to ETERNUS.')
                          % {'eternus_pool':eternus_pool})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

            else:
                try:
                    tppoollist = self.conn.EnumerateInstanceNames(
                        'FUJITSU_ThinProvisioningPool')
                    rgpoollist = self.conn.EnumerateInstanceNames(
                        'FUJITSU_RAIDStoragePool')
                except:
                    msg=(_('_find_pool,'
                           'eternus_pool:%(eternus_pool)s,'
                           'EnumerateInstanceNames,'
                           'cannot connect to ETERNUS.')
                          % {'eternus_pool':eternus_pool})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)
            # end of if

            for tppool in tppoollist:
                if str(poolinstanceid) == str(tppool['InstanceID']):
                    poolinstance = tppool
                    break
                # end of if
            else:
                for rgpool in rgpoollist:
                    if str(poolinstanceid) == str(rgpool['InstanceID']):
                        poolinstance = rgpool
                        break
                    # end of if
                # end of for rgpoollist
            # end of for tppoollist
        # end of if
        LOG.debug(_('*****_find_pool,'
                    'poolinstance: %(poolinstance)s,'
                    'Exit method.')
                   % {'poolinstance': str(poolinstance)})

        return poolinstance

    #----------------------------------------------------------------------------------------------#
    # Method : _find_eternus_service                                                               #
    #         summary      : find CIM instance                                                     #
    #         parameters   : classname                                                             #
    #         return-value : CIM instance                                                          #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _find_eternus_service(self, classname):
        '''
        find CIM instance
        '''
        #ret      : CIM instance
        #services : CIM instance service name

        LOG.debug(_('*****_find_eternus_service,'
                    'classname:%(a)s,'
                    'Enter method')
                   % {'a':str(classname)})

        #initialize
        ret      = None
        services = None

        #main processing
        try:
            services = self.conn.EnumerateInstanceNames(
                str(classname))
        except:
            msg=(_('_find_eternus_service,'
                   'classname:%(classname)s,'
                   'EnumerateInstanceNames,'
                   'cannot connect to ETERNUS.')
                  % {'classname':str(classname)})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        ret = services[0]
        LOG.debug(_('*****_find_eternus_service,'
                    'classname:%(classname)s,'
                    'ret:%(ret)s,'
                    'Exit method')
                   % {'classname':classname,
                      'ret':(str(ret))})
        return ret

    #----------------------------------------------------------------------------------------------#
    # Method : _exec_eternus_service                                                               #
    #         summary      : Execute SMI-S Method                                                  #
    #         parameters   : classname, instanceNameList, param_dict                               #
    #         return-value : status code, error description, data                                  #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _exec_eternus_service(self, classname, instanceNameList, **param_dict):
        '''
        Execute SMI-S Method
        '''
        #rc       : result of InvokeMethod
        #retdata  : return data
        #errordesc: error description

        LOG.debug(_('*****_exec_eternus_service,'
                    'classname:%(a)s,'
                    'instanceNameList:%(b)s,'
                    'paramters:%(c)s,'
                    'Enter method')
                   % {'a':str(classname),
                      'b':str(instanceNameList),
                      'c':str(param_dict)})

        #initialize
        rc        = None
        retdata   = None
        errordesc = None

        #main processing
        #use InvokeMethod
        try:
            rc, retdata = self.conn.InvokeMethod(
                classname,
                instanceNameList,
                **param_dict)
        except:
            if rc is None:
                msg=(_('_exec_eternus_service,'
                       'classname:%(classname)s,'
                       'InvokeMethod,'
                       'cannot connect to ETERNUS.')
                      % {'classname':str(classname)})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if
        #convert errorcode to error description
        try:
            errordesc = RETCODE_dic[str(rc)]
        except:
            errordesc = 'Undefined Error!!'
        ret = (rc, errordesc, retdata)

        LOG.debug(_('*****_exec_eternus_service,'
                    'classname:%(a)s,'
                    'instanceNameList:%(b)s,'
                    'paramters:%(c)s,'
                    'ret code:%(rc)s,'
                    'error description:%(errordesc)s,'
                    'Exit method')
                   % {'a':str(classname),
                      'b':str(instanceNameList),
                      'c':str(param_dict),
                      'rc':str(rc),
                      'errordesc':errordesc})

        return ret


    #----------------------------------------------------------------------------------------------#
    # Method : _find_lun                                                                           #
    #         summary      : find lun instance from volume class or volumename on ETERNUS.         #
    #         parameters   : volume                                                                #
    #         return-value : volume instance                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _find_lun(self, volumename):
        '''
        find lun instance from volume class or volumename on ETERNUS.
        '''
        #volumename    : volume name on ETERNUS
        #namelist      : volume list
        #name          : volume instanceName
        #vol_instance  : volume instance for temp
        #volumeinstance: volume instance for return

        LOG.debug(_('*****_find_lun,Enter method'))

        #initialize
        namelist       = []
        name           = None
        vol_instance   = None
        volumeinstance = None

        #main processing
        LOG.debug(_('*****_find_lun,'
                    'volumename:%(volumename)s')
                   % {'volumename':volumename})

        #get volume instance from volumename on ETERNUS
        try:
            namelist = self.conn.EnumerateInstanceNames(
                'FUJITSU_StorageVolume')
        except:
            msg=(_('_find_lun,'
                   'volumename:%(volumename)s,'
                   'EnumerateInstanceNames,'
                   'cannot connect to ETERNUS.')
                  % {'volumename':volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        for name in namelist:
            try:
                vol_instance = self.conn.GetInstance(
                    name)
            except:
                msg=(_('_find_lun,'
                       'volumename:%(volumename)s,'
                       'GetInstance,'
                       'cannot connect to ETERNUS.')
                      % {'volumename':volumename})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if vol_instance['ElementName'] == volumename:
                volumeinstance = vol_instance

                LOG.debug(_('*****_find_lun,'
                            'volumename:%(volumename)s,'
                            'vol_instance:%(vol_instance)s.')
                         % {'volumename': volumename,
                            'vol_instance': str(volumeinstance.path)})
                break
            # end of if
        else:
            LOG.debug(_('*****_find_lun,'
                        'volumename:%(volumename)s,'
                        'volume not found on ETERNUS.')
                       % {'volumename': volumename})
        # end of for namelist

        LOG.debug(_('*****_find_lun,Exit method'))

        #return volume instance
        return volumeinstance


    #----------------------------------------------------------------------------------------------#
    # Method : _find_copysession                                                                   #
    #         summary      : find copysession from volumename on ETERNUS                           #
    #         parameters   : volumename                                                            #
    #         return-value : volume instance                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _find_copysession(self, volumename):
        '''
        find copysession from volumename on ETERNUS
        '''
        #cpsession              : copysession
        #vol_instance           : volume instance
        #repservice             : FUJITSU_ReplicationService
        #rc                     : Invoke Method return code
        #replicarellist         : copysession information list
        #replicales             : copysession information
        #snapshot_vol_instance  : snapshot volume instance
        #msg                    : message
        #errordesc              : error description
        #cpsession_instance     : copysession instance

        LOG.debug(_('*****_find_copysession,'
                    'volumename:%s,'
                    'Enter method')
                   % volumename)

        #initialize
        cpsession             = None
        vol_instance          = None
        repservice            = None
        rc                    = 0
        replicarellist        = None
        replicarel            = None
        snapshot_vol_instance = None
        msg                   = None
        errordesc             = None
        cpsession_instance    = None
        #main processing
        vol_instance = self._find_lun(volumename)
        if vol_instance is not None:
            #find target_volume

            #get copysession list
            repservice = self._find_eternus_service(REPL)
            if repservice is None:
                msg = (_('_find_copysession,'
                         'Cannot find Replication Service to '
                         'find copysession'))
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if


            #find copysession for source_volume
            while True:
                LOG.debug(_('*****_find_copysession,source_volume while copysession'))
                cpsession = None

                rc, errordesc, replicarellist = self._exec_eternus_service(
                    'GetReplicationRelationships',
                    repservice,
                    Type=pywbem.Uint16(2),
                    Mode=pywbem.Uint16(2),
                    Locality=pywbem.Uint16(2))

                if rc != 0L:
                    msg = (_('_find_copysession,'
                             'source_volumename:%(volumename)s,'
                             'Return code:%(rc)lu,'
                             'Error:%(errordesc)s')
                            % {'volumename': volumename,
                               'rc': rc,
                               'errordesc':errordesc})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)
                # end of if

                for replicarel in replicarellist['Synchronizations']:
                    LOG.debug(_('*****_find_copysession,'
                                'source_volume,'
                                'replicarel:%(replicarel)s')
                              % {'replicarel':replicarel})
                    try:
                        snapshot_vol_instance = self.conn.GetInstance(
                            replicarel['SystemElement'],
                            LocalOnly=False)
                    except:
                        msg=(_('_find_copysession,'
                               'source_volumename:%(volumename)s,'
                               'GetInstance,'
                               'cannot connect to ETERNUS.')
                              % {'volumename': volumename})
                        LOG.error(msg)
                        raise exception.VolumeBackendAPIException(data=msg)

                    LOG.debug(_('*****_find_copysession,'
                                'snapshot ElementName:%(elementname)s,'
                                'source_volumename:%(volumename)s')
                               % {'elementname': snapshot_vol_instance['ElementName'],
                                  'volumename': volumename})

                    if volumename == snapshot_vol_instance['ElementName']:
                        #find copysession
                        cpsession = replicarel
                        LOG.debug(_('*****_find_copysession,'
                                    'volumename:%(volumename)s,'
                                    'Storage Synchronized instance:%(sync)s')
                                 % {'volumename': volumename,
                                    'sync': str(cpsession)})
                        msg=(_('_find_copy_session,'
                               'source_volumename:%(volumename)s,'
                               'wait for end of copysession')
                              % {'volumename': volumename})
                        LOG.info(msg)

                        try:
                            cpsession_instance = self.conn.GetInstance(
                                replicarel)
                        except:
                            break

                        LOG.debug(_('*****_find_copysession,'
                                    'status:%(status)s')
                                  % {'status':cpsession_instance['CopyState']})
                        if cpsession_instance['CopyState'] == BROKEN:
                            msg=(_('_find_copysession,'
                                   'source_volumename:%(volumename)s,'
                                   'copysession state is BROKEN')
                                  % {'volumename': volumename})
                            LOG.error(msg)
                            raise exception.VolumeBackendAPIException(data=msg)
                        # end of if
                        time.sleep(10)
                        break
                    # end of if
                else:
                    LOG.debug(_('*****_find_copysession,'
                                'volumename:%(volumename)s,'
                                'Storage Synchronized not found.')
                               % {'volumename': volumename})
                # end of for replicarellist
                if cpsession is None:
                    break
            # end of while

            #find copysession for target_volume
            for replicarel in replicarellist['Synchronizations']:
                LOG.debug(_('*****_find_copysession,'
                            'replicarel:%(replicarel)s')
                          % {'replicarel':replicarel})

                #target volume
                try:
                    snapshot_vol_instance = self.conn.GetInstance(
                        replicarel['SyncedElement'],
                        LocalOnly=False)
                except:
                    msg=(_('_find_copysession,'
                           'target_volumename:%(volumename)s,'
                           'GetInstance,'
                           'cannot connect to ETERNUS.')
                          % {'volumename': volumename})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)
                LOG.debug(_('*****_find_copysession,'
                            'snapshot ElementName:%(elementname)s,'
                            'volumename:%(volumename)s')
                           % {'elementname': snapshot_vol_instance['ElementName'],
                              'volumename': volumename})

                if volumename == snapshot_vol_instance['ElementName']:
                    #find copysession
                    cpsession = replicarel
                    LOG.debug(_('*****_find_copysession,'
                                'volumename:%(volumename)s,'
                                'Storage Synchronized instance:%(sync)s')
                             % {'volumename': volumename,
                                'sync': str(cpsession)})
                    break
                # end of if

            else:
                LOG.debug(_('*****_find_copysession,'
                            'volumename:%(volumename)s,'
                            'Storage Synchronized not found.')
                           % {'volumename': volumename})
            # end of for replicarellist

        else:
            #does not find target_volume of copysession
            msg = (_('_find_copysession,'
                     'volumename:%(volumename)s,'
                     'not found.')
                    % {'volumename':volumename})
            LOG.info(msg)
        # end of if

        LOG.debug(_('*****_find_copysession,Exit method'))

        return cpsession


    #----------------------------------------------------------------------------------------------#
    # Method : _delete_copysession                                                                 #
    #         summary      : delete copysession                                                    #
    #         parameters   : copysession                                                           #
    #         return-value :                                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _delete_copysession(self,cpsession):
        '''
        delete copysession
        '''
        #cpsession         : copysession
        #snapshot_instance : copysession instance
        #operation         : 8  stop OPC and EC
        #                  : 19 stop SnapOPC
        #repjservice       : FUJITSU_ReplicationService
        #msg               : message
        #rc                : result of invoke method
        #errordesc         : error message
        #job               : unused

        LOG.debug(_('*****_delete_copysession,Entering'))
        LOG.debug(_('*****_delete_copysession,[%s]') % cpsession)

        #initialize
        operation         = 0
        snapshot_instance = None
        repservice        = None
        msg               = None
        rc                = 0
        errordesc         = None
        job               = None

        #main processing

        #get copysession type
        # 4:SnapOPC, 5:OPC
        try:
            snapshot_instance = self.conn.GetInstance(
                cpsession,
                LocalOnly=False)
        except:
            msg=(_('_delete_copysession,'
                   'copysession:%(cpsession)s,'
                   'GetInstance,'
                   'cannot connect to ETERNUS.')
                  % {'cpsession':cpsession})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        copytype = snapshot_instance['CopyType']

        #set oparation code
        # 19:SnapOPC. 8:OPC
        operation = OPERATION_dic[copytype]

        repservice = self._find_eternus_service(REPL)
        if repservice is None:
            msg = (_('_delete_copysession,'
                     'Cannot find Replication Service to '
                     'delete copysession'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        #Invoke delete copysession method
        rc, errordesc, job = self._exec_eternus_service(
            'ModifyReplicaSynchronization',
            repservice,
            Operation=pywbem.Uint16(operation),
            Synchronization=cpsession,
            Force=True,
            WaitForCopyState=pywbem.Uint16(15))

        LOG.debug(_('*****_delete_copysession,'
                    'copysession:%(cpsession)s,'
                    'operation:%(operation)s,'
                    'Return code:%(rc)lu,'
                    'errordesc:%(errordesc)s,'
                    'Exit method')
                   % {'cpsession': cpsession,
                      'operation': operation,
                      'rc': rc,
                      'errordesc': errordesc})

        if rc != 0L:
            msg = (_('_delete_copysession,'
                     'copysession:%(cpsession)s,'
                     'operation:%(operation)s,'
                     'Return code:%(rc)lu,'
                     'Error:%(errordesc)s')
                    % {'cpsession': cpsession,
                       'operation': operation,
                       'rc': rc,
                       'errordesc': errordesc})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        return

    #----------------------------------------------------------------------------------------------#
    # Method : _get_target_portid                                                                  #
    #         summary      : return target_portid                                                  #
    #         parameters   : connector                                                             #
    #         return-value :                                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _get_target_portid(self, connector):
        '''
        return target_portid
        '''
        #target_portidlist : target_portid list
        #tgtportlist       : target port list
        #tgtport           : target port

        LOG.debug(_('*****_get_target_portid,Enter method'))

        #initialize
        target_portidlist = []
        tgtportlist       = []
        tgtport           = None

        #main processing
        if self.protocol == 'fc':
            #Protocol id FibreChannel
            try:
                tgtportlist = self.conn.EnumerateInstances(
                    'FUJITSU_SCSIProtocolEndpoint')
            except:
                msg=(_('_get_target_portid,'
                       'connector:%(connector)s,'
                       'EnumerateInstances,'
                       'cannot connect to ETERNUS.')
                      % {'connector':connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for tgtport in tgtportlist:
                if tgtport['ConnectionType'] == 2:
                    target_portidlist.append(tgtport['Name'])

                LOG.debug(_('*****_get_target_portid,'
                            'wwn:%(wwn)s,'
                            'connection type:%(cont)s,'
                            'ramode:%(ramode)s')
                           % {'wwn': tgtport['Name'],
                              'cont': tgtport['ConnectionType'],
                              'ramode': tgtport['RAMode']})
            # end of for tgtportlist

            LOG.debug(_('*****_get_target_portid,'
                        'target wwns: %(target_portid)s ')
                       % {'target_portid': target_portidlist})

        elif self.protocol == 'iSCSI':
            #Protocol is iSCSI
            try:
                tgtportlist = self.conn.EnumerateInstances(
                    'FUJITSU_iSCSIProtocolEndpoint')
            except:
                msg=(_('_get_target_portid,'
                       'connector:%(connector)s,'
                       'EnumerateInstances,'
                       'cannot connect to ETERNUS.')
                      % {'connector':connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for tgtport in tgtportlist:
                if tgtport['ConnectionType'] == 7:
                    target_portidlist.append(tgtport['Name'])
                LOG.debug(_('*****_get_target_portid,'
                            'iSCSIname:%(iscsiname)s,'
                            'connection type:%(cont)s,'
                            'ramode: %(ramode)s')
                           % {'iscsiname': tgtport['Name'],
                              'cont': tgtport['ConnectionType'],
                              'ramode': tgtport['RAMode']})
            # end of for tgtportlist

            LOG.debug(_('*****_get_target_portid,'
                        'target iSCSIname: %(target_portid)s ')
                       % {'target_portid': target_portidlist})
        # end of if

        if len(target_portidlist) == 0:
            msg = (_('_get_target_portid,'
                     'protcol:%(protocol)s,'
                     'connector:%(connector)s,'
                     'target_portid does not found.')
                   % {'protocol': self.protocol,
                      'connector': connector})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if

        LOG.debug(_('*****_get_target_portid,Exit method'))

        return target_portidlist


    #----------------------------------------------------------------------------------------------#
    # Method : map_lun                                                                             #
    #         summary      : map volume to host                                                    #
    #         parameters   : volume, connector, targetlist                                         #
    #         return-value :                                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _map_lun(self, volume, connector, targetlist = []):
        '''
        map volume to host.
        '''
        #vol_instance : volume instance
        #volumename   : volume name on ETERNUS
        #volume_uid   : volume UID
        #               ex)600000E00D110000001104EC00DB0000
        #initiatorlist: inisiator id
        #               ex)[u'10000000c978c574', u'10000000c978c575']
        #targetlist   : ETERNUS target id
        #               ex)[u'500000E0D444EC80', u'500000E0D444EC81']
        #aglist       : assigned AffinityGroup list
        #ag           : AffinityGroup
        #configservice: FUJITSU_StorageConfigurationService
        #msg          : message
        #rc           : result of invoke method
        #errordesc    : error message
        #job          : unused

        LOG.debug(_('*****_map_lun,'
                    'volume:%(volume)s,'
                    'connector:%(con)s,'
                    'Enter method')
                   % {'volume': volume['display_name'],
                      'con': connector})

        #initialize
        vol_instance  = None
        volumename    = None
        volume_uid    = None
        initiatorlist = []
        target        = None
        aglist        = []
        ag            = None
        configservice = None
        msg           = None
        rc            = 0
        errordesc     = None
        job           = None

        #main processing
        volumename    = self._create_volume_name(volume['id'])
        vol_instance  = self._find_lun(volumename)
        volume_uid    = vol_instance['Name']
        initiatorlist = self._find_initiator_names(connector)
        aglist        = self._find_affinity_group(connector)
        configservice = self._find_eternus_service(CTRL_CONF)

        if len(targetlist) == 0:
            targetlist = self._get_target_portid(connector)
        # end of if

        if configservice is None:
            msg = (_('_map_lun,'
                     'vol_instance.path:%(vol)s,'
                     'volumename:%(volumename)s,'
                     'volume_uid:%(uid)s,'
                     'initiator:%(initiator)s,'
                     'target:%(tgt)s',
                     'aglist:%(aglist)s',
                     'Cannot find Controller Configuration')
                    % {'vol': str(vol_instance.path),
                       'volumename': [volumename],
                       'uid': [volume_uid],
                       'initiator': initiatorlist,
                       'tgt': targetlist,
                       'aglist': aglist})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if
        LOG.debug(_('*****_map_lun,'
                    'vol_instance.path:%(vol)s,'
                    'volumename:%(volumename)s,'
                    'initiator:%(initiator)s,'
                    'target:%(tgt)s')
                  % {'vol': str(vol_instance.path),
                     'volumename': [volumename],
                     'initiator': initiatorlist,
                     'tgt': targetlist})

        if len(aglist) == 0:
            #create new affinity group
            for target in targetlist:
                LOG.debug(_('*****_map_lun,'
                            'lun_name:%(volume_uid)s,'
                            'Initiator:%(initiator)s,'
                            'target:%(tgt)s')
                           % {'volume_uid': [volume_uid],
                              'initiator': initiatorlist,
                              'tgt': target})

                rc, errordesc, job = self._exec_eternus_service(
                    'ExposePaths',
                    configservice,
                    LUNames=[volume_uid],
                    InitiatorPortIDs=initiatorlist,
                    TargetPortIDs=[target],
                    DeviceAccesses=[pywbem.Uint16(2)])

                LOG.debug(_('*****_map_lun,'
                            'errordesc:%(errordesc)s,'
                            'rc:%(rc)s,'
                            'Create new affinitygroup')
                           % {'errordesc':errordesc,
                              'rc':rc})

                if rc != 0L:
                    msg = (_('_map_lun,'
                             'lun_name:%(volume_uid)s,'
                             'Initiator:%(initiator)s,'
                             'target:%(tgt)s,'
                             'Return code:%(rc)lu,'
                             'Error:%(errordesc)s')
                            % {'volume_uid': [volume_uid],
                               'initiator': initiatorlist,
                               'tgt': target,
                               'rc': rc,
                               'errordesc':errordesc})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)
                # end of if
            # end of for targetlist
        else:
            #add lun to affinity group
            for ag in aglist:
                LOG.debug(_('*****_map_lun,'
                            'ag:%(ag)s,'
                            'lun_name:%(volume_uid)s')
                           % {'ag': str(ag),
                              'volume_uid':volume_uid})

                rc, errordesc, job = self._exec_eternus_service(
                    'ExposePaths',
                    configservice, LUNames=[volume_uid],
                    DeviceAccesses=[pywbem.Uint16(2)],
                    ProtocolControllers=[ag])

                LOG.debug(_('*****_map_lun,'
                            'errordesc:%(errordesc)s,'
                            'rc:%(rc)s,'
                            'Add lun affinitygroup')
                           % {'errordesc':errordesc,
                              'rc':rc})

                if rc != 0L:
                    msg = (_('_map_lun,'
                             'lun_name:%(volume_uid)s,'
                             'Initiator:%(initiator)s,'
                             'target:%(tgt)s,'
                             'Return code:%(rc)lu,'
                             'Error:%(errordesc)s')
                            % {'volume_uid': [volume_uid],
                               'initiator': initiatorlist,
                               'tgt': target,
                               'rc': rc,
                               'errordesc':errordesc})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)
                # end of if
            # end of for aglist
        # end of if
        LOG.debug(_('*****_map_lun,'
                    'volumename:%(volumename)s,'
                    'Exit method')
                   % {'volumename':volumename})
        return

    #----------------------------------------------------------------------------------------------#
    # Method : _find_initiator_names                                                               #
    #         summary      : return initiator names                                                #
    #         parameters   : connector                                                             #
    #         return-value : initiator name                                                        #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _find_initiator_names(self, connector):
        '''
        return initiator names
        '''
        #initiatornamelist : initiator name
        #msg               : None

        LOG.debug(_('*****_find_initiator_names,Enter method'))

        #initialize
        initiatornamelist = []
        msg               = None

        #main processing
        if self.protocol == 'iSCSI' and connector['initiator'] is not None:
            initiatornamelist.append(connector['initiator'])
        elif self.protocol == 'fc' and connector['wwpns'] is not None:
            initiatornamelist = connector['wwpns']
        # end of if
        if len(initiatornamelist) == 0:
            msg = (_('_find_initiator_names,'
                     'connector:%(connector)s,'
                     'not found initiator')
                    % {'connector':connector})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if
        LOG.debug(_('*****_find_initiator_names,'
                    'initiator:%(initiator)s.'
                    'Exit method')
                  % {'initiator': initiatornamelist})

        return initiatornamelist


    #----------------------------------------------------------------------------------------------#
    # Method : _find_affinity_group                                                                #
    #         summary      : return affinity group from connector                                  #
    #         parameters   : connector, vol_instance                                               #
    #         return-value : initiator name                                                        #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _find_affinity_group(self,connector,vol_instance=None):
        '''
        find affinity group from connector
        '''
        #affinity_grouplist: affinity group list(return value)
        #initiatorlist     : initiator list
        #initiator         : initiator
        #aglist            : affinity group list(temp)
        #ag                : affinity group
        #hostaglist        : host affinity group information listr
        #hostag            : host affinity group information

        LOG.debug(_('*****_find_affinity_group,'
                    'Enter method'))

        #initialize
        affinity_grouplist  = []
        initiatorlist       = []
        initiator           = None
        aglist              = []
        ag                  = None
        hostaglist          = []
        hostag              = None

        #main processing
        initiatorlist = self._find_initiator_names(connector)

        if vol_instance is None:
            try:
                aglist = self.conn.EnumerateInstanceNames(
                    'FUJITSU_AffinityGroupController')
            except:
                msg=(_('_find_affinity_group,'
                       'connector:%(connector)s,'
                       'EnumerateInstanceNames,'
                       'cannot connect to ETERNUS.')
                      % {'connector':connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug(_('*****_find_affinity_group,'
                        'affinity_groups:%s')
                       % aglist)
        else:
            try:
                aglist = self.conn.AssociatorNames(
                    vol_instance.path,
                    AssocClass ='CIM_ProtocolControllerForUnit',
                    ResultClass='FUJITSU_AffinityGroupController')
            except:
                msg=(_('_find_affinity_group,'
                       'connector:%(connector)s,'
                       'AssociatorNames,'
                       'cannot connect to ETERNUS.')
                      % {'connector':connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug(_('*****_find_affinity_group,'
                        'vol_instance.path:%(vol)s,'
                        'affinity_groups:%(aglist)s')
                       % {'vol':vol_instance.path,
                          'aglist':aglist})
        # end of if
        for ag in aglist:
            try:
                hostaglist = self.conn.Associators(
                    ag,
                    AssocClass ='CIM_AuthorizedTarget',
                    ResultClass='FUJITSU_AuthorizedPrivilege')
            except:
                msg=(_('_find_affinity_group,'
                       'connector:%(connector)s,'
                       'Associators,'
                       'cannot connect to ETERNUS.')
                      % {'connector':connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for hostag in hostaglist:
                for initiator in initiatorlist:
                    if initiator.lower() not in hostag['InstanceID'].lower():
                        continue
                    # end of if
                    LOG.debug(_('*****_find_affinity_group,'
                                'AffinityGroup:%(ag)s')
                               % {'ag':ag})
                    affinity_grouplist.append(ag)
                    break
                # end of for initiatorlist
                break
            # end of for hostaglist
        # end of for aglist
        LOG.debug(_('*****_find_affinity_group,'
                    'initiators:%(initiator)s,'
                    'affinity_group:%(affinity_group)s.'
                    'Exit method')
                   % {'initiator': initiatorlist,
                      'affinity_group': affinity_grouplist})

        return affinity_grouplist

    #----------------------------------------------------------------------------------------------#
    # Method : _unmap_lun                                                                          #
    #         summary      : unmap volume from host                                                #
    #         parameters   : volume, connector                                                     #
    #         return-value :                                                                       #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _unmap_lun(self, volume, connector):
        '''
        unmap volume from host
        '''

        #vol_instance : volume instance
        #volumename   : volume name on ETERNUS
        #volume_uid   : volume UID
        #               ex)600000E00D110000001104EC00DB0000
        #volmap       : volume map information
        #aglist       : assigned AffinityGroup list
        #ag           : AffinityGroup
        #configservice: FUJITSU_StorageConfigurationService
        #msg          : message
        #rc           : result of invoke method
        #errordesc    : error message
        #job          : unused

        LOG.debug(_('*****_unmap_lun,Enter method'))

        #initialize
        vol_instance   = None
        volumename     = None
        volume_uid     = None
        volmap         = {}
        device_number  = None
        configservice  = None
        msg            = None
        aglist         = None
        ag             = None
        msg            = None
        rc             = 0
        errordesc      = None
        job            = None

        #main processing
        volumename    = self._create_volume_name(volume['id'])
        vol_instance  = self._find_lun(volumename)
        if vol_instance is None:
            LOG.info(_('_unmap_lun,'
                       'volumename:%(volumename)s,'
                       'volume not found.'
                       'Exit method')
                      % {'volumename':volumename})
            return
        # end of if
        volume_uid    = vol_instance['Name']
        volmap        = self._find_device_number(volume, connector)
        configservice = self._find_eternus_service(CTRL_CONF)
        aglist        = self._find_affinity_group(connector,vol_instance)

        device_number = volmap['target_lun']
        if device_number is None:
            LOG.info(_('_unmap_lun,'
                       'volumename:%(volumename)s,'
                       'volume is not mapped.'
                       'Exit method')
                      % {'volumename':volumename})
            return
        # end of if
        if configservice is None:
            msg = (_('_unmap_lun,'
                     'vol_instance.path:%(vol)s,'
                     'volumename:%(volumename)s,'
                     'volume_uid:%(uid)s,'
                     'aglist:%(aglist)s',
                     'Cannot find Controller Configuration')
                    % {'vol': str(vol_instance.path),
                       'volumename': [volumename],
                       'uid': [volume_uid],
                       'aglist': aglist})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of if
        for ag in aglist:
            LOG.debug(_('*****_unmap_lun,'
                        'volumename:%(volumename)s,'
                        'volume_uid:%(volume_uid)s,'
                        'AffinityGroup:%(ag)s')
                       % {'volumename': volumename,
                          'volume_uid': volume_uid,
                          'ag': ag})

            rc, errordesc, job = self._exec_eternus_service(
                'HidePaths',
                configservice,
                LUNames=[volume_uid],
                ProtocolControllers=[ag])

            LOG.debug(_('*****_unmap_lun,'
                        'errordesc:%(errordesc)s,'
                        'rc:%(rc)s')
                       % {'errordesc':errordesc,
                          'rc':rc})

            if rc != 0L:
                msg = (_('_unmap_lun,'
                         'volumename:%(volumename)s,'
                         'volume_uid:%(volume_uid)s,'
                         'AffinityGroup:%(ag)s,'
                         'Return Code:%(rc)lu,'
                         'Error:%(errordesc)s')
                        % {'volumename': volumename,
                           'volume_uid': volume_uid,
                           'ag': ag,
                           'rc': rc,
                           'errordesc':errordesc})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # end of if
        # end of for aglist
        LOG.debug(_('*****_unmap_lun,'
                    'volumename:%(volumename)s,'
                    'Exit method')
                   % {'volumename':volumename})

        return

    #----------------------------------------------------------------------------------------------#
    # Method : _get_iqn                                                                            #
    #         summary      : get target iqn                                                        #
    #         parameters   : iscsi ip address                                                      #
    #         return-value : iqn                                                                   #
    #         exceptions   :                                                                       #
    #----------------------------------------------------------------------------------------------#
    def _get_iqn(self):
        '''
        get target port iqn
        '''
        #iscsiip              : target iscsi ip address
        #ip_endpointlist      : ip protocol endpoint list
        #ip_endpoint          : ip protocol endpoint
        #tcp_endpointlist     : tcp protocol endpoint list
        #tcp_endpoint         : tcp protocol endpoint
        #iscsi_endpointlist   : iscsi protocol endpoint list
        #iscsi_endpoint       : iscsi protocol endpoint
        #ip_endpoint_instance : ip protocol endpoint instance

        LOG.debug(_('*****_get_iqn,Enter method'))

        #initialize
        iscsiip              = self.configuration.iscsi_ip_address
        ip_endpointlist      = []
        ip_endpoint          = None
        tcp_endpointlist     = []
        tcp_endpoint         = None
        iscsi_endpointlist   = []
        iscsi_endpoint       = None
        ip_endpoint_instance = None

        if iscsiip is None:
            iscsiip = self._get_drvcfg('EternusISCSIIP')
        # end of if
        try:
            ip_endpointlist = self.conn.EnumerateInstanceNames(
                'FUJITSU_IPProtocolEndpoint')
        except:
            msg=(_('_get_iqn,'
                   'iscsiip:%(iscsiip)s,'
                   'EnumerateInstanceNames,'
                   'cannot connect to ETERNUS.')
                  % {'iscsiip':iscsiip})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        for ip_endpoint in ip_endpointlist:
            try:
                ip_endpoint_instance = self.conn.GetInstance(
                    ip_endpoint)
                LOG.debug(_('*****_get_iqn,'
                            'ip_endpoint_instance[IPv4Address]:%(ip_endpoint_instance)s,'
                            'iscsiip:%(iscsiip)s')
                           % {'ip_endpoint_instance':ip_endpoint_instance['IPv4Address'],
                              'iscsiip':iscsiip})
            except:
                msg=(_('_get_iqn,'
                       'iscsiip:%(iscsiip)s,'
                       'GetInstance,'
                       'cannot connect to ETERNUS.')
                      % {'iscsiip':iscsiip})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if ip_endpoint_instance['IPv4Address'] != iscsiip:
                continue
            else:
                LOG.debug(_('*****_get_iqn,find iscsiip'))
                try:
                    tcp_endpointlist = self.conn.AssociatorNames(
                        ip_endpoint,
                        ResultClass='FUJITSU_TCPProtocolEndpoint')
                except:
                    msg=(_('_get_iqn,'
                           'iscsiip:%(iscsiip)s,'
                           'AssociatorNames,'
                           'cannot connect to ETERNUS.')
                          % {'iscsiip':iscsiip})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

                for tcp_endpoint in tcp_endpointlist:
                    try:
                        iscsi_endpointlist = self.conn.Associators(
                            tcp_endpoint,
                            ResultClass='FUJITSU_iSCSIProtocolEndpoint')
                    except:
                        msg=(_('_get_iqn,'
                               'iscsiip:%(iscsiip)s,'
                               'AssociatorNames,'
                               'cannot connect to ETERNUS.')
                              % {'iscsiip':iscsiip})
                        LOG.error(msg)
                        raise exception.VolumeBackendAPIException(data=msg)

                    for iscsi_endpoint in iscsi_endpointlist:
                        iqn = iscsi_endpoint['Name'].split(',')[0]
                        LOG.debug(_('*****_get_target_portid,'
                                    'iscsi_endpoint[Name]:%(iscsi_endpoint)s')
                                   % {'iscsi_endpoint':iscsi_endpoint['Name']})
                        break
                    # end of for iscsi_endpointlist
                    break
                # end of for tcp_endpointlist
            # end of if
            break
        # end of for ip_endpoint
        else:
            msg = (_('_get_iqn,'
                     'iscsiip:%(iscsiip)s,'
                     'not found iqn')
                    % {'iscsiip':iscsiip})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        # end of for ip_endpointlist
        LOG.debug(_('*****_get_iqn,%s,Exit method') % iqn )
        return iqn

