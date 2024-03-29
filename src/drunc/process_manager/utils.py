
def generate_process_query(f, at_least_one:bool, all_processes_by_default:bool=False):
    import click

    @click.pass_context
    def new_func(ctx, session, name, user, uuid, **kwargs):
        is_trivial_query = bool((len(uuid) == 0) and (session is None) and (len(name) == 0) and (user is None))
        # print(f'is_trivial_query={is_trivial_query} ({type(is_trivial_query)}): uuid={uuid} ({type(uuid)}), session={session} ({type(session)}), name={name} ({type(name)}), user={user} ({type(user)})')

        if is_trivial_query and at_least_one:
            raise click.BadParameter('You need to provide at least a \'--uuid\', \'--session\', \'--user\' or \'--name\'!')

        if all_processes_by_default and is_trivial_query:
            name = ['.*']

        from druncschema.process_manager_pb2 import ProcessUUID, ProcessQuery

        uuids = [ProcessUUID(uuid=uuid_) for uuid_ in uuid]

        query = ProcessQuery(
            session = session,
            names = name,
            user = user,
            uuids = uuids,
        )
        #print(query)
        return ctx.invoke(f, query=query,**kwargs)

    from functools import update_wrapper
    return update_wrapper(new_func, f)



def tabulate_process_instance_list(pil, title, long=False):
    from rich.table import Table
    t = Table(title=title)
    t.add_column('session')
    t.add_column('user')
    t.add_column('friendly name')
    t.add_column('uuid')
    t.add_column('alive')
    t.add_column('exit-code')
    if long:
        t.add_column('executable')

    for result in pil.values:
        m = result.process_description.metadata
        row = [m.session, m.user, m.name, result.uuid.uuid]

        from druncschema.process_manager_pb2 import ProcessInstance
        alive = 'True' if result.status_code == ProcessInstance.StatusCode.RUNNING else '[danger]False[/danger]'

        row += [alive, f'{result.return_code}']
        if long:
            executables = [e.exec for e in result.process_description.executable_and_arguments]
            row += ['; '.join(executables)]
        t.add_row(*row)

    return t
