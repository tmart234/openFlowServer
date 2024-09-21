# openFlowServer
A backend server, designed around Raspberry Pi OS, for [OpenFlow](https://github.com/tmart234/openFlow): An API designed to deliver pre-processed NASA SMAP soil moisture data and USGS Vegdri data

```mermaid
graph TD
    A[External Data Sources] -->|Raw SMAP and VegDRI data| B(1. Data Acquisition & Processing Service)
    B -->|Processed data| C[(2. SQLite Database)]
    C <-->|Data retrieval/storage| D(3. API)
    D -->|Requested data| E[Mobile Apps]
    D -->|Training datasets| F[AI Training Systems]
    G[Ansible] -->|Configuration| B
    H[System Startup] -->|Initialize| B
    H -->|Initialize| C
    H -->|Initialize| D
```
