"""
Desabilita verificação SSL globalmente para o container dbt.
Necessário em redes corporativas com proxy que re-assinam certificados.
O Python executa este arquivo automaticamente antes de qualquer outro código.
"""
import os
import ssl
import urllib3
import requests

os.environ["PYTHONHTTPSVERIFY"] = "0"

ssl._create_default_https_context = ssl._create_unverified_context

try:
    
    urllib3.disable_warnings()
except ImportError:
    pass

# dbt deps usa requests internamente — patch obrigatório para bypasear SSL no proxy corporativo
try:
    
    _original_send = requests.Session.send
    def _patched_send(self, *args, **kwargs):
        kwargs["verify"] = False
        return _original_send(self, *args, **kwargs)
    requests.Session.send = _patched_send
except ImportError:
    pass
