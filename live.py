from fastapi import FastAPI
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


# Boto3를 사용하여 DynamoDB 서비스에 연결
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1') 
ivs_client = boto3.client('ivs', region_name='ap-northeast-1')

# DynamoDB 테이블 객체 생성
table = dynamodb.Table('Test-Live-DB') 
app = FastAPI()

@app.get("/api/live/{channelid}")
async def read_item_by_channelid(channelid: str):
    response = table.query(
        IndexName='channelid-index',
        KeyConditionExpression=Key('channelid').eq(channelid),
        # ProjectionExpression='userid'
    )
    items = response.get('Items', [])
    if not items:
        return {"error": "채널이 없습니다."}

    full_channel_arn = f"arn:aws:ivs:ap-northeast-1:891377305172:channel/{channelid}"

    try:
        ivs_response = ivs_client.get_stream(channelArn=full_channel_arn)
        # 스트림 정보에서 필요한 값 추출 및 합치기
        live_data = {
            'LiveData': {
                **items[0],  # DynamoDB 데이터를 LiveData에 직접 통합
                'viewerCount': ivs_response['stream'].get('viewerCount') if 'stream' in ivs_response else None
            }
        }
    except ClientError as e:
        if e.response['Error']['Code'] == 'ChannelNotBroadcasting':
            # 채널이 온라인 상태가 아닌 경우에만 특정 메시지 반환
            return {"error": "채널이 온라인이 아닙니다"}
        else:
            # 다른 AWS 클라이언트 에러 처리
            return {"error": e.response['Error']['Message']}

    return live_data
