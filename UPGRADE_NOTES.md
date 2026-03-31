# DATV-Linux Upgrade Notes

This source package is intended for public GitHub release use.

## Packaging

- `cmake --install` installs the transmitter binary, GUI launcher scripts, dependency checker, README, license, manual PDF, requirements file, and bundled test pattern.
- Mock or lab-only files are intentionally not included in this public package.

## Pluto paths

- Stock ADALM-Pluto / Pluto+ uses the direct libiio path.
- Pluto firmware paths use GUI-side MQTT handoff logic.
- The CLI binary remains a direct SDR backend path and does not replace the GUI firmware handoff logic.

## Low symbol-rate operation

- Low-SR stabilization is optional and remains off by default.
- Keep Pluto firmware current when operating above 250 kS/s.
