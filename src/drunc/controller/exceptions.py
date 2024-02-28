from drunc.exceptions import DruncCommandException

class ControllerException(DruncCommandException):
    pass

class CannotSurrenderControl(ControllerException):
    pass

class OtherUserAlreadyInControl(ControllerException):
    def __init__(self, user_who_wants_to_execute, user_in_control=None, system=None, instance=None):

        txt = f'\'{user_who_wants_to_execute}\' is not in control'

        if instance is not None:
            txt += f' of the {instance}'

        if system is not None:
            txt += f' ({system})'

        if user_in_control is not None:
            if user_in_control == '':
                txt += ', no one is'
            else:
                txt += f', \'{user_in_control}\' is'


        super().__init__(txt)

class MalformedMessage(ControllerException):
    pass