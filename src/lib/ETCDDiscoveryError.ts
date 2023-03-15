import {ExError} from '@sora-soft/framework';

export enum ETCDDiscoveryErrorCode {
  ERR_COMPONENT_NOT_FOND = 'ERR_COMPONENT_NOT_FOND',
  ERR_SERVICE_NOT_FOUND = 'ERR_SERVICE_NOT_FOUND',
}

class ETCDDiscoveryError extends ExError {
  constructor(code: ETCDDiscoveryErrorCode, message: string) {
    super(code, 'ETCDDiscoveryError', message);
    Object.setPrototypeOf(this, ETCDDiscoveryError.prototype);
    Error.captureStackTrace(this, this.constructor);
  }
}

export {ETCDDiscoveryError};
