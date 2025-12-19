# Only needed if you want custom plugins beyond defaults
FROM grafana/grafana:11.0.0
COPY grafana_plugins /var/lib/grafana/plugins
USER root
RUN grafana-cli plugins install grafana-clock-panel grafana-worldmap-panel
USER grafana
