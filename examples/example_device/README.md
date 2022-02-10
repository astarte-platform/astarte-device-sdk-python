# Astarte Device SDK Python Example device
This is an example of how to use the Device SDK to connect a device to Astarte and send
data on `datastream` or`properties` interfaces.

## Usage
### 1. Device registration and credentials secret emission
If the device is already registered, skip this section.

The device must be registered beforehand to obtain its credentials-secret.

1. Using the astartectl command [astartectl](https://github.com/astarte-platform/astartectl).
2. Using the [Astarte Dashboard](https://docs.astarte-platform.org/snapshot/015-astarte_dashboard.html),
which is located at `https://dashboard.<your-astarte-domain>.`

### 2. Run example
Before running the example the following constants must have a value at 
the start of `example_device.py`

```python
_DEVICE_ID = 'DEVICE_ID_HERE'
_REALM = 'REALM_HERE'
_CREDENTIAL_SECRET = 'CREDENTIAL_SECRET_HERE'
_PAIRING_URL = 'https://api.astarte.EXAMPLE.COM/pairing'
_PERSISTENCY_DIR = tempfile.gettempdir()
```

Then run 
```shell
pip install -e ../../
python example_device.py
```


