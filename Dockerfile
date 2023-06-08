FROM python:3.10.4-bullseye

SHELL ["/bin/bash", "-c"]

RUN apt install git

RUN git clone https://github.com/DUNE-DAQ/drunc.git -bplasorak/better-process-queries # remove that branch after it has been merged in develop
RUN cd drunc && pip install -r requirements.txt && pip install .

ENV DRUNC_DIR=/drunc
ENV DRUNC_DATA=/drunc/data