import json
from indroid.utils import config
from indroid.server.db import DB
from functools import wraps

def delayer(sender, *args, **kwargs):
    """Resend the message to the sender-pipe. This handles storing the message and retrieving it later."""
    if '__properties__' in kwargs:
        props = kwargs['__properties__']
        if 'destination' in props and props['destination'] == '.'.join([__name__, delayer.__name__]):
        # The original destination of the resend is resend itself. Replace with original address:
            if 'destination_old' in props and props['destination_old'] != props['destination']:
                # Replace with original destination:
                props['destination'] = props['destination_old']
            else:
                # Original destination is new destination. Hopeless, don't resend:
                return True
    sender.resend(*args, **kwargs)
    return True

@config(r"C:\indroid\ini\indroid.ini")
def with_db(config, f):
    db = DB(config.server.database)
    @wraps(f)
    def wrapped(*args, **kwargs):
        return f(db, *args, **kwargs)
    return wrapped

@with_db
def logger(db, sender, *args, **kwargs):
    """Log the incoming message to a specified destination. At this moment, a call is made to the specified database."""
    # Clean up superflous keys: 
    for k in ('__routing_key__', '__key__'):
        if k in kwargs:
            del kwargs[k]
    # Open database and write records:    
    db.log(*args, **kwargs)
    return True