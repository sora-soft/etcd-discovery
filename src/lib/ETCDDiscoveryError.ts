import {ExError} from '@sora-soft/framework';

export enum ETCDDiscoveryErrorCode {
  ERR_COMPONENT_NOT_FOND = 'ERR_COMPONENT_NOT_FOND',
}

class ETCDDiscoveryError extends ExError {
  constructor(code: ETCDDiscoveryErrorCode, message: string) {
    super(code, 'ETCDDiscoveryError', message);
    Object.setPrototypeOf(this, ETCDDiscoveryError.prototype);
    Error.captureStackTrace(this, this.constructor);
  }
}

export {ETCDDiscoveryError};
