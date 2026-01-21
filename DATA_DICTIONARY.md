# Data Dictionary

## Top-Level Folders (SDC3/)

| Folder | Description | Typical Size |
|--------|-------------|---------------|
| **atmo/** | Atmospheric emission & propagation models used in the simulation pipeline. | 42.92 GB |
| **data/** | Simulation control data, intermediate products, configuration-dependent files. | 41.85 GB |
| **frg/** | Astrophysical foreground components (Galactic + extragalactic). | 6.12 GB |
| **exgf/** | Extended Galactic foregrounds with large angular structure. | 3.04 GB |
| **gf/** | Galactic foreground subsets (smaller segments). | 3.08 GB |
| **image/** | Reference overview images, maps, diagnostics. | 653 KB |
| **lightcones/** | Cosmological lightcone cubes for EoR structure (HI distribution). | 837 MB |
| **ms/** | Main interferometric visibilities in CASA MS format. | 6.27 TB |

## MeasurementSet Subtables

All `.MS` folders contain the same 14 subtables:

- ANTENNA
- DATA_DESCRIPTION
- FEED
- FIELD
- FLAG_CMD
- HISTORY
- OBSERVATION
- PHASED_ARRAY
- POINTING
- POLARIZATION
- PROCESSOR
- SOURCE
- SPECTRAL_WINDOW
- STATE

These encode telescope geometry, spectral configuration, polarization setup, observation metadata, calibration information, and processing history.
