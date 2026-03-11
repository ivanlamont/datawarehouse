$Region = "us-east-1"

aws cloudformation deploy `
  --region $Region `
  --template-file infra/daily-seeder.yml `
  --stack-name daily-seeder `
  --capabilities CAPABILITY_IAM `
  --parameter-overrides `
    VpcId=vpc-0f087cf4a4b92a30f `
    SubnetIds=subnet-026b23bdbcf0fe25b,subnet-02fa37b69b8cf5806 `
    RdsSecurityGroupId=sg-0f9b1665fd66328c8 `
    RdsEndpoint=production-investrlot-db.c9is6sooiqa4.us-east-1.rds.amazonaws.com `
    DatabaseName=investrlot `
    DatabaseUser=investrlotadmin `
    DbPasswordSecretArn=arn:aws:secretsmanager:us-east-1:779388075365:secret:investrlot/production/database/password-MW4OY3 `
    PolygonSecretArn=arn:aws:secretsmanager:us-east-1:779388075365:secret:ml4t/polygon-credentials-9D88iv `
    SeedUniverse=BOTH `
    TiingoSecretArn=arn:aws:secretsmanager:us-east-1:779388075365:secret:ml4t/tiingo-api-key-Mj0mUE
    
