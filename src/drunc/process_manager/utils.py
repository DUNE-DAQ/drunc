
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

def flatten_tree(tree, prefix=''):
    lines = []

    try:
        for node in tree.children:
            lines.append(f"{prefix}{node.label}")
            lines.extend(flatten_tree(node, prefix + "  "))
        return lines
    except AttributeError:
        pass
        

def make_tree(pil, long=False):
    from rich.tree import Tree

    session_name = None
    session_trees = None
    last_drunc_controller = None

    for result in pil.values:
        m = result.process_description.metadata
        env = result.process_description.executable_and_arguments
        for execu in env:
            if execu.exec == "drunc-controller":
                if not session_trees:
                    session_name = Tree("") 
                    session_trees = session_name.add(m.name)
                    last_drunc_controller = session_trees
                else:
                    last_drunc_controller = session_trees.add(m.name)
            elif execu.exec == "daq_application" and last_drunc_controller:
                last_drunc_controller.add(m.name)
    tree_lines = flatten_tree(session_name)
    return tree_lines

def tabulate_process_instance_list(pil, title, long=False):
    from rich.table import Table

    t = Table(title=title)
    t.add_column('session')
    t.add_column('friendly name')
    t.add_column('user')
    t.add_column('host')
    t.add_column('uuid')
    t.add_column('alive')
    t.add_column('exit-code')
    if long:
        t.add_column('executable')
    tree_str = make_tree(pil, long)
    try:
        for result, line in zip(pil.values, tree_str):
            m = result.process_description.metadata
            host = result.process_restriction.allowed_hosts[0] #temporary whilst we figure out a way of getting the actual hosts by asking the precoesses themselves
            row = [m.session, line, m.user, host, result.uuid.uuid]
            from druncschema.process_manager_pb2 import ProcessInstance
            alive = 'True' if result.status_code == ProcessInstance.StatusCode.RUNNING else '[danger]False[/danger]'
            row += [alive, f'{result.return_code}']
            if long:
                executables = [e.exec for e in result.process_description.executable_and_arguments]
                row += ['; '.join(executables)]
            t.add_row(*row)
    except TypeError:
        for result in pil.values:
            m = result.process_description.metadata
            host = result.process_restriction.allowed_hosts[0]
            row = [m.session, m.name, m.user, host ,result.uuid.uuid]
            from druncschema.process_manager_pb2 import ProcessInstance
            alive = 'True' if result.status_code == ProcessInstance.StatusCode.RUNNING else '[danger]False[/danger]'

            row += [alive, f'{result.return_code}']
            if long:
                executables = [e.exec for e in result.process_description.executable_and_arguments]
                row += ['; '.join(executables)]
            t.add_row(*row)
    return t
