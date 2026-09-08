ARG BUILD_FROM
FROM $BUILD_FROM

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

COPY rootfs /
RUN chmod a+x /run.sh

HEALTHCHECK --interval=60s --timeout=5s --start-period=60s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:8099/health || exit 1

CMD [ "/run.sh" ]
