# Certificados

## `sectigo_public_server_auth_ca_dv_r36.pem`

Certificado intermediário da Sectigo usado pelo `DOEMGHook`.

O portal `www.jornalminasgerais.mg.gov.br` envia apenas o certificado folha na
cadeia TLS; o intermediário (`Sectigo Public Server Authentication CA DV R36`,
emitido por `Sectigo Public Server Authentication Root R46`) não é enviado.
Sem ele, o `requests` falha com `SSLError` — mesmo com o `certifi` atualizado.

O hook monta em tempo de execução um bundle com o `certifi` do ambiente e este
arquivo, e o informa em `verify` (ver `DOEMGHook._ca_bundle_path`). A verificação
TLS permanece habilitada; `verify=False` não é usado.

- Origem: `http://crt.sectigo.com/SectigoPublicServerAuthenticationCADVR36.crt`
  (obtido via AIA do certificado do portal em 16/11/2026).
- Validade: 22/03/2021 a 21/03/2036.
- Atualização: baixar novamente a partir do AIA do certificado do portal e
  substituir este arquivo; o bundle é regenerado automaticamente pelo hash do
  conteúdo.
