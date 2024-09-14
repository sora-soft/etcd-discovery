import {Discovery, ExError, IListenerMetaData, INodeMetaData, IServiceMetaData, IWorkerMetaData, Logger, QueueExecutor, Runtime, SubscriptionManager} from '@sora-soft/framework';
import {EtcdComponent, EtcdElection, EtcdEvent} from '@sora-soft/etcd-component';
import {IKeyValue, IOptions, Lease, Watcher, Etcd3} from '@sora-soft/etcd-component/etcd3';
import {ETCDDiscoveryError, ETCDDiscoveryErrorCode} from './ETCDDiscoveryError.js';
import {TypeGuard} from '@sora-soft/type-guard';
import {readFile} from 'fs/promises';
import {fromEvent} from '@sora-soft/framework/rxjs';

const pkg = JSON.parse(
  await readFile(new URL('../../package.json', import.meta.url), {encoding: 'utf-8'})
) as {version: string};

export interface ITECDWorkerMetaData extends IWorkerMetaData {
  version: string;
  createRevision: string;
  modRevision: string;
}

export interface IETCDServiceMetaData extends IServiceMetaData {
  version: string;
  createRevision: string;
  modRevision: string;
}

export interface IETCDEndpointMetaData extends IListenerMetaData {
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
  prefix: string;
}

export type IETCDOptions = IOptions;

class ETCDDiscovery extends Discovery {

  constructor(options: IETCDDiscoveryOptions) {
    super();
    TypeGuard.assert<IETCDDiscoveryOptions>(options);
    this.options_ = options;
    this.remoteServiceIdMap_ = new Map();
    this.localServiceIdMap_ = new Map();
    this.remoteListenerIdMap_ = new Map();
    this.remoteNodeListMap_ = new Map();
    this.localWorkerIdMap_ = new Map();
    this.remoteWorkerIdMap_ = new Map();
    this.localListenerIdMap_ = new Map();
    this.localNodeIdMap_ = new Map();
    this.executor_ = new QueueExecutor();
    this.subManager_ = new SubscriptionManager();
  }

  async startup() {
    this.component_ = Runtime.getComponent<EtcdComponent>(this.options_.etcdComponentName);
    if (!this.component_)
      throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_COMPONENT_NOT_FOND, 'ERR_COMPONENT_NOT_FOND');

    await this.component_.start();
    this.component_.emitter.on(EtcdEvent.LeaseReconnect, async (lease) => {
      Runtime.frameLogger.warn('etcd-discovery', {event: 'etcd-lease-reconnected'});
      this.lease_ = lease;
      for(const [_, service] of this.localServiceIdMap_) {
        this.registerService(service).catch((err: ExError) => {
          Runtime.frameLogger.error('etcd-discovery', err, {event: 'register-service', error: Logger.errorMessage(err)});
        });
      }

      for (const [_, worker] of this.localWorkerIdMap_) {
        this.registerWorker(worker).catch((err: ExError) => {
          Runtime.frameLogger.error('etcd-discovery', err, {event: 'register-worker', error: Logger.errorMessage(err)});
        });
      }

      for (const [_, listener] of this.localListenerIdMap_) {
        this.registerEndpoint(listener).catch((err: ExError) => {
          Runtime.frameLogger.error('etcd-discovery', err, {event: 'register-listener', error: Logger.errorMessage(err)});
        });
      }

      for (const [_, node] of this.localNodeIdMap_) {
        this.registerNode(node).catch((err: ExError) => {
          Runtime.frameLogger.error('etcd-discovery', err, {event: 'register-node', error: Logger.errorMessage(err)});
        });
      }
    });

    this.etcd_ = this.component_.client;
    this.lease_ = this.component_.lease;

    this.workerListWatcher_ = await this.etcd_.watch().prefix(`${this.workerPrefix}`).create();
    const workerPutSub = fromEvent(this.workerListWatcher_, 'put').subscribe(([kv]: IKeyValue[]) => {
      this.executor_.doJob(async () => {
        this.updateWorkerMeta(kv);
      }).catch((err: ExError) => {
        Runtime.frameLogger.error('etcd-discovery', err, {event: 'update-worker-meta-error', error: Logger.errorMessage(err)});
      });
    });
    this.subManager_.register(workerPutSub);

    const workerDelSub = fromEvent(this.workerListWatcher_, 'delete').subscribe(([kv]: IKeyValue[]) => {
      this.executor_.doJob(async () => {
        const key = kv.key.toString();
        const id = key.slice(this.workerPrefix.length + 1);
        this.deleteWorkerMeta(id);
      }).catch((err: ExError) => {
        Runtime.frameLogger.error('etcd-discovery', err, {event: 'delete-worker-meta-error', error: Logger.errorMessage(err)});
      });
    });
    this.subManager_.register(workerDelSub);

    this.serviceListWatcher_ = await this.etcd_.watch().prefix(`${this.servicePrefix}`).create();

    const servicePutSub = fromEvent(this.serviceListWatcher_, 'put', ).subscribe(([kv]: IKeyValue[]) => {
      this.executor_.doJob(async () => {
        this.updateServiceMeta(kv);
      }).catch((err: ExError) => {
        Runtime.frameLogger.error('etcd-discovery', err, {event: 'update-service-meta-error', error: Logger.errorMessage(err)});
      });
    });
    this.subManager_.register(servicePutSub);

    const serviceDelSub = fromEvent(this.serviceListWatcher_, 'delete').subscribe(([kv]: IKeyValue[]) => {
      this.executor_.doJob(async () => {
        const key = kv.key.toString();
        const id = key.slice(this.servicePrefix.length + 1);
        this.deleteServiceMeta(id);
      }).catch((err: ExError) => {
        Runtime.frameLogger.error('etcd-discovery', err, {event: 'delete-service-meta-error', error: Logger.errorMessage(err)});
      });
    });
    this.subManager_.register(serviceDelSub);

    this.listenerListWatcher_ = await this.etcd_.watch().prefix(this.endpointPrefix).create();
    const listenerPutSub = fromEvent(this.listenerListWatcher_, 'put').subscribe(([kv]: IKeyValue[]) => {
      this.executor_.doJob(async () => {
        this.updateEndpointMeta(kv);
      }).catch((err: ExError) => {
        Runtime.frameLogger.error('etcd-discovery', err, {event: 'update-endpoint-meta-error', error: Logger.errorMessage(err)});
      });
    });
    this.subManager_.register(listenerPutSub);

    const listenerDelSub = fromEvent(this.listenerListWatcher_, 'delete').subscribe(([kv]: IKeyValue[]) => {
      this.executor_.doJob(async () => {
        const key = kv.key.toString();
        const id = key.slice(this.endpointPrefix.length + 1);

        this.deleteEndpointMeta(id);
      }).catch((err: ExError) => {
        Runtime.frameLogger.error('etcd-discovery', err, {event: 'delete-endpoint-meta-error', error: Logger.errorMessage(err)});
      });
    });
    this.subManager_.register(listenerDelSub);

    this.nodeListWatcher_ = await this.etcd_.watch().prefix(this.nodePrefix).create();
    const nodePutSub = fromEvent(this.nodeListWatcher_, 'put').subscribe(([kv]: IKeyValue[]) => {
      this.executor_.doJob(async () => {
        this.updateNodeMeta(kv);
      }).catch((err: ExError) => {
        Runtime.frameLogger.error('etcd-discovery', err, {event: 'update-node-meta-error', error: Logger.errorMessage(err)});
      });
    });
    this.subManager_.register(nodePutSub);

    const nodeDelSub = fromEvent(this.nodeListWatcher_, 'delete').subscribe(([kv]: IKeyValue[]) => {
      this.executor_.doJob(async () => {
        const key = kv.key.toString();
        const id = key.slice(this.nodePrefix.length + 1);
        this.deleteNodeMeta(id);
      }).catch((err: ExError) => {
        Runtime.frameLogger.error('etcd-discovery', err, {event: 'delete-node-meta-error', error: Logger.errorMessage(err)});
      });
    });
    this.subManager_.register(nodeDelSub);

    await this.init();

    this.executor_.start();
  }

  get info() {
    return {
      type: 'etcd',
      version: this.version,
    };
  }

  get version() {
    return pkg.version;
  }

  protected updateEndpointMeta(kv: IKeyValue) {
    const key = kv.key.toString();
    const meta = JSON.parse(kv.value.toString()) as IListenerMetaData;

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
    };

    this.remoteListenerIdMap_.set(id, {
      ...data,
      version: kv.version,
      modRevision: kv.mod_revision,
      createRevision: kv.create_revision,
    });
    this.pushListenerUpdate();
  }

  protected updateWorkerMeta(kv: IKeyValue) {
    const key = kv.key.toString();
    const meta = JSON.parse(kv.value.toString()) as IWorkerMetaData;

    const id = key.slice(this.workerPrefix.length + 1);
    const existed = this.remoteWorkerIdMap_.get(id);

    if (existed && existed.modRevision >= kv.mod_revision)
      return;

    this.remoteWorkerIdMap_.set(id, {
      ...meta,
      version: kv.version,
      modRevision: kv.mod_revision,
      createRevision: kv.create_revision
    });
    this.pushWorkerUpdate();
  }

  protected updateServiceMeta(kv: IKeyValue) {
    const key = kv.key.toString();
    const meta = JSON.parse(kv.value.toString()) as IServiceMetaData;

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
    this.pushServiceUpdate();
  }

  protected updateNodeMeta(kv: IKeyValue) {
    const key = kv.key.toString();
    const meta = JSON.parse(kv.value.toString()) as INodeMetaData;

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
    this.pushNodeUpdate();
  }

  protected deleteNodeMeta(id: string) {
    const info = this.remoteNodeListMap_.get(id);
    if (!info)
      return;

    this.remoteNodeListMap_.delete(id);
    this.pushNodeUpdate();
  }

  protected deleteServiceMeta(id: string) {
    const info = this.remoteServiceIdMap_.get(id);
    if (!info)
      return;

    this.remoteServiceIdMap_.delete(id);
    this.pushServiceUpdate();
  }

  protected deleteWorkerMeta(id: string) {
    const info = this.remoteWorkerIdMap_.get(id);
    if (!info)
      return;

    this.remoteWorkerIdMap_.delete(id);
    this.pushWorkerUpdate();
  }

  protected deleteEndpointMeta(id: string) {
    const info = this.remoteListenerIdMap_.get(id);
    if (!info)
      return;

    this.remoteListenerIdMap_.delete(id);
    this.pushListenerUpdate();
  }

  async getAllWorkerList() {
    return [...this.remoteWorkerIdMap_].map(([_, info]) => info);
  }

  async getAllNodeList() {
    return [...this.remoteNodeListMap_].map(([_, info]) => info);
  }

  async getAllServiceList() {
    return [...this.remoteServiceIdMap_].map(([_, info]) => info);
  }

  async getAllEndpointList(): Promise<IListenerMetaData[]> {
    return [...this.remoteListenerIdMap_].map(([_, info]) => info);
  }

  async getEndpointList(service: string) {
    const serviceList = await this.getServiceList(service);
    const idList = serviceList.map(v => v.id);
    return [...this.remoteListenerIdMap_].map(([_, info]) => {
      return info;
    }).filter((info) => {
      return idList.includes(info.targetId);
    });
  }

  async getServiceList(name: string) {
    return [...this.remoteServiceIdMap_].map(([_, info]) => {
      return info;
    }).filter((info) => {
      return info.name === name;
    });
  }

  async getWorkerList(name: string) {
    return [...this.remoteWorkerIdMap_].map(([_, info]) => {
      return info;
    }).filter((info) => {
      return info.name === name;
    });
  }

  async getWorkerById(id: string) {
    const worker = this.remoteWorkerIdMap_.get(id);
    if (!worker)
      throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_WORKER_NOT_FOUND, 'ERR_WORKER_NOT_FOUND');
    return worker;
  }

  async getNodeById(id: string) {
    const node = this.remoteNodeListMap_.get(id);
    return node;
  }

  async getEndpointById(id: string) {
    const endpoint = this.remoteListenerIdMap_.get(id);
    return endpoint;
  }

  async getServiceById(id: string) {
    const service = this.remoteServiceIdMap_.get(id);
    return service;
  }

  async getNodeList() {
    return [...this.remoteNodeListMap_].map(([_, info]) => {
      return info;
    });
  }

  async shutdown() {
    await this.executor_.stop();
    if (this.lease_) {
      await this.lease_.revoke();
      this.lease_ = undefined;
    }
    this.nodeSubject_.complete();
    this.workerSubject_.complete();
    this.serviceSubject_.complete();
    this.listenerSubject_.complete();
    this.subManager_.destroy();
  }

  async registerService(meta: IServiceMetaData) {
    await this.executor_.doJob(async () => {
      if (!this.lease_)
        throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_ETCD_NOT_CONNECTED, 'ERR_ETCD_NOT_CONNECTED');
      await this.lease_.put(`${this.servicePrefix}/${meta.id}`).value(JSON.stringify(meta)).exec();
      this.localServiceIdMap_.set(meta.id, meta);
    });
  }

  async unregisterService(id: string) {
    await this.executor_.doJob(async () => {
      if (!this.etcd_)
        throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_ETCD_NOT_CONNECTED, 'ERR_ETCD_NOT_CONNECTED');
      await this.etcd_.delete().key(`${this.servicePrefix}/${id}`).exec();
      this.localServiceIdMap_.delete(id);
    });
  }

  async registerWorker(meta: IWorkerMetaData): Promise<void> {
    await this.executor_.doJob(async () => {
      if (!this.lease_)
        throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_ETCD_NOT_CONNECTED, 'ERR_ETCD_NOT_CONNECTED');
      await this.lease_.put(`${this.workerPrefix}/${meta.id}`).value(JSON.stringify(meta)).exec();
      this.localWorkerIdMap_.set(meta.id, meta);
    });
  }

  async unregisterWorker(id: string): Promise<void> {
    await this.executor_.doJob(async () => {
      if (!this.etcd_)
        throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_ETCD_NOT_CONNECTED, 'ERR_ETCD_NOT_CONNECTED');
      await this.etcd_.delete().key(`${this.workerPrefix}/${id}`).exec();
      this.localWorkerIdMap_.delete(id);
    });
  }

  async registerNode(node: INodeMetaData) {
    await this.executor_.doJob(async () => {
      if (!this.lease_)
        throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_ETCD_NOT_CONNECTED, 'ERR_ETCD_NOT_CONNECTED');
      await this.lease_.put(`${this.nodePrefix}/${node.id}`).value(JSON.stringify(node)).exec();
      this.localNodeIdMap_.set(node.id, node);
    });
  }

  async unregisterNode(id: string) {
    await this.executor_.doJob(async () => {
      if (!this.etcd_)
        throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_ETCD_NOT_CONNECTED, 'ERR_ETCD_NOT_CONNECTED');
      await this.etcd_.delete().key(`${this.endpointPrefix}/${id}`).exec();
      this.localNodeIdMap_.delete(id);
    });
  }

  async registerEndpoint(info: IListenerMetaData) {
    await this.executor_.doJob(async () => {
      if (!this.lease_)
        throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_ETCD_NOT_CONNECTED, 'ERR_ETCD_NOT_CONNECTED');
      await this.lease_.put(`${this.endpointPrefix}/${info.id}`).value(JSON.stringify(info)).exec();
      this.localListenerIdMap_.set(info.id, info);
    });
  }

  async unregisterEndPoint(id: string) {
    await this.executor_.doJob(async () => {
      if (!this.etcd_)
        throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_ETCD_NOT_CONNECTED, 'ERR_ETCD_NOT_CONNECTED');
      await this.etcd_.delete().key(`${this.endpointPrefix}/${id}`).exec();
      this.localListenerIdMap_.delete(id);
    });
  }

  createElection(name: string) {
    if (!this.etcd_)
      throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_ETCD_NOT_CONNECTED, 'ERR_ETCD_NOT_CONNECTED');
    return new EtcdElection(this.etcd_, `${this.singletonPrefix}/${name}`);
  }

  private async init() {
    if (!this.etcd_)
      throw new ETCDDiscoveryError(ETCDDiscoveryErrorCode.ERR_ETCD_NOT_CONNECTED, 'ERR_ETCD_NOT_CONNECTED');
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

  private pushServiceUpdate() {
    this.serviceSubject_.next([...this.remoteServiceIdMap_].map(([_, service]) => service));
  }

  private pushNodeUpdate() {
    this.nodeSubject_.next([...this.remoteNodeListMap_].map(([_, node]) => node));
  }

  private pushListenerUpdate() {
    this.listenerSubject_.next([...this.remoteListenerIdMap_].map(([_, listener]) => listener));
  }

  private pushWorkerUpdate() {
    this.workerSubject_.next([...this.remoteWorkerIdMap_].map(([_, worker]) => worker));
  }

  private get workerPrefix() {
    return `${this.options_.prefix}/worker`;
  }

  private get servicePrefix() {
    return `${this.options_.prefix}/service`;
  }

  private get nodePrefix() {
    return `${this.options_.prefix}/node`;
  }

  private get endpointPrefix() {
    return `${this.options_.prefix}/endpoint`;
  }

  private get singletonPrefix() {
    return `${this.options_.prefix}/singleton`;
  }

  private component_?: EtcdComponent;
  private etcd_?: Etcd3;
  private options_: IETCDDiscoveryOptions;
  private lease_?: Lease;
  private workerListWatcher_?: Watcher;
  private serviceListWatcher_?: Watcher;
  private listenerListWatcher_?: Watcher;
  private nodeListWatcher_?: Watcher;

  private remoteServiceIdMap_: Map<string, IETCDServiceMetaData>;
  private localServiceIdMap_: Map<string, IServiceMetaData>;
  private remoteWorkerIdMap_: Map<string, ITECDWorkerMetaData>;
  private localWorkerIdMap_: Map<string, IWorkerMetaData>;
  private remoteNodeListMap_: Map<string, IETCDNodeMetaData>;
  private localNodeIdMap_: Map<string, INodeMetaData>;
  private remoteListenerIdMap_: Map<string, IETCDEndpointMetaData>;
  private localListenerIdMap_: Map<string, IListenerMetaData>;

  private executor_: QueueExecutor;
  private subManager_: SubscriptionManager;
}

export {ETCDDiscovery};
