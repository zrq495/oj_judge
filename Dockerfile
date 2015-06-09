FROM python:2.7
ENV C_FORCE_ROOT 1
RUN mkdir /work
RUN chmod -R 777 /work
ADD . /oj_judge
WORKDIR /oj_judge/Lo-runner
RUN python setup.py install
WORKDIR /oj_judge
RUN pip install -r requirements.txt
