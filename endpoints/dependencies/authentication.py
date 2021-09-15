from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from . import credentials

#########################################################
# handle authentication
#########################################################

security = HTTPBasic()

def get_auth(header: HTTPBasicCredentials = Depends(security)):
    auth = credentials.authenticate(header.username, header.password)
    if not auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    if auth["metered"] and auth["calls"] > 1500:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS)
    return auth["user"]
