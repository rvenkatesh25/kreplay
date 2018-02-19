FROM 767177213176.dkr.ecr.us-east-1.amazonaws.com/tt/python2:stable

ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
ADD . /app

CMD ["./run.py"]
