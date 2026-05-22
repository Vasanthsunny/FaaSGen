# building to local registeries
cd qr-f1-gen
docker build -t 172.0.0.0:5000/qr-f1-gen:latest .
docker push  172.0.0.0:5000/qr-f1-gen:latest
