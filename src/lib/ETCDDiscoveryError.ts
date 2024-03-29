import {ExError} from '@sora-soft/framework';

export enum ETCDDiscoveryErrorCode {
  ERR_COMPONENT_NOT_FOND = 'ERR_COMPONENT_NOT_FOND',
  ERR_SERVICE_NOT_FOUND = 'ERR_SERVICE_NOT_FOUND',
  ERR_WORKER_NOT_FOUND = 'ERR_WORKER_NOT_FOUND',
  ERR_NODE_NOT_FOUND = 'ERR_NODE_NOT_FOUND',
  ERR_ENDPOINT_NOT_FOUND = 'ERR_ENDPOINT_NOT_FOUND',
  ERR_ETCD_NOT_CONNECTED = 'ERR_ETCD_NOT_CONNECTED',
}

class ETCDDiscoveryError extends ExError {
  constructor(code: ETCDDiscoveryErrorCode, message: string) {
    super(code, 'ETCDDiscoveryError', message);
    Object.setPrototypeOf(this, ETCDDiscoveryError.prototype);
    Error.captureStackTrace(this, this.constructor);
  }
}

export {ETCDDiscoveryError};
