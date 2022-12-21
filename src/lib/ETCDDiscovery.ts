import {Discovery, DiscoveryEvent, DiscoveryListenerEvent, DiscoveryNodeEvent, DiscoveryServiceEvent, IListenerEventData, IListenerMetaData, INodeMetaData, IServiceMetaData, QueueExecutor, Runtime} from '@sora-soft/framework';
import {EtcdComponent, IKeyValue, IOptions, Lease, Watcher, Etcd3} from '@sora-soft/etcd-component';
import {ETCDDiscoveryError, ETCDDiscoveryErrorCode} from './ETCDDiscoveryError';
import {EtcdEvent} from '@sora-soft/etcd-component/dist/lib/EtcdEvent';

const pkg = require('../../package.json');

export interface IETCDServiceMetaData extends IServiceMetaData {
  version: string;
  createRevision: string;
  modRevision: string;
}

export interface IETCDEndpointMetaData extends IListenerEventData {
  version: string;
  createRevision: string;
  modRevision: string;
}

export interface IETCDNodeMetaData extends INodeMetaData {
  version: string;
  createRevision: string;
  modRevision: string;
}

export interface IETCDDiscoveryOptions {
  etcdComponentName: string;
  ttl: number;
  prefix: string;
}

export type IETCDOptions = IOptions;

class ETCDDiscovery extends Discovery {

  constructor(options: IETCDDiscoveryOptions) {
    super();
    this.options_ = options;
    this.remoteServiceIdMap_ = new Map();
    this.localServiceIdMap_ = new Map();
    this.remoteListenerIdMap_ = new Map();
    this.remoteNodeListMap_ = new Map();
    this.executor_ = new QueueExecutor();
  }

  async startup() {
    this.component_ = Runtime.getComponent<EtcdComponent>(this.options_.etcdComponentName);
    if (!this.component_)
      throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_COMPONENT_NOT_FOND, `ERR_COMPONENT_NOT_FOND`);

    await this.component_.start();
    this.component_.emitter.on(EtcdEvent.LeaseReconnect, (lease) => {
      Runtime.frameLogger.warn('etcd-discovery', {event: 'etcd-lost-lease'});
      this.lease_ = lease;
      this.discoveryEmitter_.emit(DiscoveryEvent.DiscoveryReconnect);
    });

    this.etcd_ = this.component_.client;
    this.lease_ = this.component_.lease;

    this.serviceListWatcher_ = await this.etcd_.watch().prefix(`${this.servicePrefix}`).create();
    this.serviceListWatcher_.on('put', (kv) => {
      this.executor_.doJob(async () => {
        this.updateServiceMeta(kv);
      });
    });
    this.serviceListWatcher_.on('delete', (kv) => {
      this.executor_.doJob(async () => {
        const key = kv.key.toString();
        const id = key.slice(this.servicePrefix.length + 1);
        this.deleteServiceMeta(id);
      });
    });

    this.endpointListWatcher_ = await this.etcd_.watch().prefix(this.endpointPrefix).create();
    this.endpointListWatcher_.on('put', (kv) => {
      this.executor_.doJob(async () => {
        this.updateEndpointMeta(kv);
      });
    });
    this.endpointListWatcher_.on('delete', (kv) => {
      this.executor_.doJob(async () => {
        const key = kv.key.toString();
        const id = key.slice(this.endpointPrefix.length + 1);

        this.deleteEndpointMeta(id);
      });
    });

    this.nodeListWatcher_ = await this.etcd_.watch().prefix(this.nodePrefix).create();
    this.nodeListWatcher_.on('put', (kv) => {
      this.executor_.doJob(async () => {
        this.updateNodeMeta(kv);
      })
    })

    await this.init();

    await this.executor_.start();
  }

  get info() {
    return {
      type: 'etcd',
      version: this.version,
    }
  }

  get version() {
    return pkg.version;
  }

  protected updateEndpointMeta(kv: IKeyValue) {
    const key = kv.key.toString();
    const meta: IListenerMetaData = JSON.parse(kv.value.toString());

    const id = key.slice(this.endpointPrefix.length + 1);
    const existed = this.remoteListenerIdMap_.get(id);

    if (existed && existed.modRevision >= kv.mod_revision)
      return;

    const service = this.remoteServiceIdMap_.get(meta.targetId);
    if (!service)
      return;

    const data = {
      ...meta,
      service: service.name,
    }

    this.remoteListenerIdMap_.set(id, {
      ...data,
      version: kv.version,
      modRevision: kv.mod_revision,
      createRevision: kv.create_revision,
    });
    if (!existed) {
      this.listenerEmitter_.emit(DiscoveryListenerEvent.ListenerCreated, data);
      Runtime.frameLogger.info('discovery', {event: 'listener-created', info: data});
    } else {
      this.listenerEmitter_.emit(DiscoveryListenerEvent.ListenerUpdated, id, data);
      Runtime.frameLogger.info('discovery', {event: 'listener-updated', id, info: data})
      if (existed.state !== meta.state) {
        this.listenerEmitter_.emit(DiscoveryListenerEvent.ListenerStateUpdate, id, meta.state, existed.state, data);
        Runtime.frameLogger.info('discovery', { event: 'listener-state-update', id, state: meta.state});
      }
    }
  }

  protected updateServiceMeta(kv: IKeyValue) {
    const key = kv.key.toString();
    const meta: IServiceMetaData = JSON.parse(kv.value.toString());

    const id = key.slice(this.servicePrefix.length + 1);
    const existed = this.remoteServiceIdMap_.get(id);

    if (existed && existed.modRevision >= kv.mod_revision)
      return;

    this.remoteServiceIdMap_.set(id, {
      ...meta,
      version: kv.version,
      modRevision: kv.mod_revision,
      createRevision: kv.create_revision
    });
    if (!existed) {
      this.serviceEmitter_.emit(DiscoveryServiceEvent.ServiceCreated, meta);
      Runtime.frameLogger.info('discovery', { event: 'service-created', id: meta.id, state: meta});
    } else {
      this.serviceEmitter_.emit(DiscoveryServiceEvent.ServiceUpdated, id, meta);
      Runtime.frameLogger.info('discovery', { event: 'service-update', id, state: meta});
      if (existed.state !== meta.state) {
        this.serviceEmitter_.emit(DiscoveryServiceEvent.ServiceStateUpdate, id, meta.state, existed.state, meta);
        Runtime.frameLogger.info('discovery', { event: 'service-state-update', id, state: meta.state});
      }
    }
  }

  protected updateNodeMeta(kv: IKeyValue) {
    const key = kv.key.toString();
    const meta: INodeMetaData = JSON.parse(kv.value.toString());

    const id = key.slice(this.nodePrefix.length + 1);
    const existed = this.remoteNodeListMap_.get(id);

    if (existed && existed.modRevision >= kv.mod_revision)
      return;

    this.remoteNodeListMap_.set(id, {
      ...meta,
      version: kv.version,
      modRevision: kv.mod_revision,
      createRevision: kv.create_revision
    });
    if (!existed) {
      this.nodeEmitter_.emit(DiscoveryNodeEvent.NodeCreated, meta);
      Runtime.frameLogger.info('discovery', {event: 'node-created', id, meta});
    } else {
      this.nodeEmitter_.emit(DiscoveryNodeEvent.NodeUpdated, id, meta);
      Runtime.frameLogger.info('discovery', {event: 'node-updated', id, meta});
      if (existed.state !== meta.state) {
        this.nodeEmitter_.emit(DiscoveryNodeEvent.NodeStateUpdate, id, meta.state, existed.state, meta);
        Runtime.frameLogger.info('discovery', {event: 'node-state-update', id, state: meta.state});
      }
    }
  }

  protected deleteServiceMeta(id: string) {
    const info = this.remoteServiceIdMap_.get(id);
    if (!info)
      return;

    this.remoteServiceIdMap_.delete(id);
    this.serviceEmitter_.emit(DiscoveryServiceEvent.ServiceDeleted, id, info);
    Runtime.frameLogger.info('discovery', { event: 'service-deleted', id, info});

  }

  protected deleteEndpointMeta(id: string) {
    const info = this.remoteListenerIdMap_.get(id);
    if (!info)
      return;

    this.remoteListenerIdMap_.delete(id);
    this.listenerEmitter_.emit(DiscoveryListenerEvent.ListenerDeleted, id, info);
    Runtime.frameLogger.info('discovery', { event: 'listener-deleted', id});
  }

  async getEndpointList(service: string) {
    const serviceList = await this.getServiceList(service);
    const idList = serviceList.map(v => v.id);
    return [...this.remoteListenerIdMap_].map(([id, info]) => {
      return info;
    }).filter((info) => {
      return idList.includes(info.targetId);
    });
  }

  async getServiceList(name: string, localOnly = false) {
    return [...this.remoteServiceIdMap_].map(([id, info]) => {
      return info;
    }).filter((info) => {
      return info.name === name;
    });
  }

  async getServiceById(id: string) {
    return this.remoteServiceIdMap_.get(id);
  }

  async getNodeList() {
    return [...this.remoteNodeListMap_].map(([id, info]) => {
      return info;
    });
  }

  async shutdown() {
    await this.lease_.revoke();
  }

  async registerService(meta: IServiceMetaData) {
    await this.executor_.doJob(async () => {
      await this.lease_.put(`${this.servicePrefix}/${meta.id}`).value(JSON.stringify(meta)).exec();
      this.localServiceIdMap_.set(meta.id, meta);
    });
  }

  async unregisterService(id: string) {
    await this.executor_.doJob(async () => {
      await this.etcd_.delete().key(`${this.servicePrefix}/${id}`).exec();
      this.localServiceIdMap_.delete(id);
    });
  }

  async registerNode(node: INodeMetaData) {
    await this.executor_.doJob(async () => {
      await this.lease_.put(`${this.nodePrefix}/${node.id}`).value(JSON.stringify(node)).exec();
    })
  }

  async unregisterNode(id: string) {
    await this.executor_.doJob(async () => {
      await this.etcd_.delete().key(`${this.endpointPrefix}/${id}`).exec();
    })
  }

  async registerEndpoint(info: IListenerMetaData) {
    await this.executor_.doJob(async () => {
      await this.lease_.put(`${this.endpointPrefix}/${info.id}`).value(JSON.stringify(info)).exec();
    });
  }

  async unregisterEndPoint(id: string) {
    await this.executor_.doJob(async () => {
      await this.etcd_.delete().key(`${this.endpointPrefix}/${id}`).exec();
    });
  }

  private async init() {
    const serviceRes = await this.etcd_.getAll().prefix(`${this.servicePrefix}`).exec();
    for (const kv of serviceRes.kvs) {
      this.updateServiceMeta(kv);
    }

    const endpointRes = await this.etcd_.getAll().prefix(this.endpointPrefix).exec();
    for (const kv of endpointRes.kvs) {
      this.updateEndpointMeta(kv);
    }

    const nodeRes = await this.etcd_.getAll().prefix(this.nodePrefix).exec();
    for (const kv of nodeRes.kvs) {
      this.updateNodeMeta(kv);
    }
  }

  private get servicePrefix() {
    return `${this.options_.prefix}/service`;
  }

  private get nodePrefix() {
    return `${this.options_.prefix}/node`;
  }

  private get endpointPrefix() {
    return `${this.options_.prefix}/endpoint`
  }

  private component_: EtcdComponent;
  private etcd_: Etcd3;
  private options_: IETCDDiscoveryOptions;
  private lease_: Lease;
  private serviceListWatcher_: Watcher;
  private endpointListWatcher_: Watcher;
  private nodeListWatcher_: Watcher;
  private remoteServiceIdMap_: Map<string, IETCDServiceMetaData>;
  private localServiceIdMap_: Map<string, IServiceMetaData>;
  private remoteListenerIdMap_: Map<string, IETCDEndpointMetaData>;
  private remoteNodeListMap_: Map<string, IETCDNodeMetaData>;
  private executor_: QueueExecutor;
}

export {ETCDDiscovery};
