
class Unauthorised(Exception):
    def __init__(self, user, action):
        self.user = user
        self.action = action
        super().__init__(f"{user} is not authorised to {action}")
