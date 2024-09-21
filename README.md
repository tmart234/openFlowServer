# openFlowServer
A backend server, designed around Raspberry Pi OS, for [OpenFlow](https://github.com/tmart234/openFlow): An API designed to deliver pre-processed NASA SMAP soil moisture data and USGS Vegdri data

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'primaryColor': '#BB2528', 'primaryTextColor': '#fff', 'primaryBorderColor': '#7C0000', 'lineColor': '#F8B229', 'secondaryColor': '#006100', 'tertiaryColor': '#fff'}}}%%
graph TD
    A[External Data Sources] -->|Raw SMAP and VegDRI data| B[Data Acquisition & Processing Service]
    B -->|Processed data| C[(SQLite Database)]
    C <-->|Data retrieval/storage| D[API]
    D -->|Requested data| E[Mobile Apps]
    D -->|Training datasets| F[AI Training Systems]
    G[Ansible] -->|Configuration| B

    classDef default fill:#BB2528,stroke:#7C0000,stroke-width:2px,color:#fff;
    classDef db fill:#006100,stroke:#004d00,stroke-width:2px,color:#fff;
    class C db;
```
