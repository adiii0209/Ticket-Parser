from app.gmail.auth import get_gmail_service

service = get_gmail_service()

response = service.users().watch(
    userId='me',
    body={
        'topicName': 'projects/ticket-parser-488310/topics/gmail_ticket',
        'labelIds': ['INBOX']
    }
).execute()

print(response)