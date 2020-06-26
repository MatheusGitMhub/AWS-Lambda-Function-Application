sam build && sam package --template-file template.yaml --output-template-file packaged.yaml --s3-bucket tesseracto-lambda-bucket && sam deploy --tags Proyecto=cosmos Cliente=anglo
