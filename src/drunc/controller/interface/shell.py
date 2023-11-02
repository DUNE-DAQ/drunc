import click
import click_shell

from drunc.controller.interface.context import ControllerContext
from drunc.utils.utils import log_levels
from drunc.utils.shell_utils import add_traceback_flag


@click_shell.shell(prompt='drunc-controller > ', chain=True)
@click.argument('controller-address', type=str)
@click.option('-t', '--traceback', is_flag=True, default=False, help='Print full exception traceback')
@click.option('-l', '--log-level', type=click.Choice(log_levels.keys(), case_sensitive=False), default='INFO', help='Set the log level')
@click.pass_context
def controller_shell(ctx, controller_address:str, log_level:str, traceback:bool) -> None:
    from drunc.utils.utils import update_log_level

    update_log_level(log_level)

    ctx.obj.reset(
        print_traceback = traceback,
        address = controller_address,
    )

    from druncschema.request_response_pb2 import Description
    desc = Description()

    ntries = 5

    from drunc.utils.grpc_utils import ServerUnreachable
    for itry in range(ntries):
        try:
            desc = ctx.obj.get_driver().describe(rethrow=True)
        except ServerUnreachable as e:
            ctx.obj.error(f'Could not connect to the controller, trial {itry+1} of {ntries}')
            if itry >= ntries-1:
                raise e
            else:
                from time import sleep
                sleep(0.5)

        except Exception as e:
            ctx.obj.critical('Could not get the controller\'s status')
            ctx.obj.critical(e)
            ctx.obj.critical('Exiting.')
            ctx.obj.terminate()
            raise e

        else:
            ctx.obj.info(f'{controller_address} is \'{desc.name}.{desc.session}\' (name.session), starting listening...')
            ctx.obj.start_listening(desc.broadcast)
            break

    ctx.obj.print('Connected to the controller')

    from druncschema.generic_pb2 import PlainText, PlainTextVector

    children = ctx.obj.get_driver().ls(rethrow=False)
    ctx.obj.print(f'{desc.name}.{desc.session}\'s children :family:: {children.text}')


    def cleanup():
        # remove the shell from the controller broadcast list
        dead = False
        import grpc
        who = ''
        from drunc.utils.grpc_utils import unpack_any
        try:
            who = ctx.obj.get_driver().who_is_in_charge(rethrow=True).text

        except grpc.RpcError as e:
            dead = grpc.StatusCode.UNAVAILABLE == e.code()
        except Exception as e:
            ctx.obj.error('Could not understand who is in charge from the controller.')
            ctx.obj.error(e)
            who = 'no_one'

        if dead:
            ctx.obj.error('Controller is dead. Exiting.')
            return

        if who == ctx.obj.get_token().user_name and ctx.obj.took_control:
            ctx.obj.info('You are in control. Surrendering control.')
            try:
                ctx.obj.get_driver().surrender_control(rethrow=True)
            except Exception as e:
                ctx.obj.error('Could not surrender control.')
                ctx.obj.error(e)
            ctx.obj.info('Control surrendered.')
        ctx.obj.terminate()

    ctx.call_on_close(cleanup)

    ctx.obj.info(f'Taking control of the controller as {ctx.obj.get_token()}')
    try:
        ctx.obj.get_driver().take_control(rethrow=True)
        ctx.obj.took_control = True

    except Exception as e:
        ctx.obj.warn('You are NOT in control.')
        ctx.obj.took_control = False
        return

    ctx.obj.info('You are in control.')

    from drunc.controller.interface.commands import (
        describe, ls, status, take_control, surrender_control, who_am_i, who_is_in_charge, fsm, include, exclude
    )
    ctx.command.add_command(describe, 'describe')
    ctx.command.add_command(ls, 'ls')
    ctx.command.add_command(status, 'status')
    ctx.command.add_command(take_control, 'take_control')
    ctx.command.add_command(surrender_control, 'surrender_control')
    ctx.command.add_command(who_am_i, 'who_am_i')
    ctx.command.add_command(who_is_in_charge, 'who_is_in_charge')
    ctx.command.add_command(fsm, 'fsm')
    ctx.command.add_command(include, 'include')
    ctx.command.add_command(exclude, 'exclude')
