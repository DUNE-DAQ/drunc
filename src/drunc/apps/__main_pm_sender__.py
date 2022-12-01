
def main():
    import nest_asyncio
    nest_asyncio.apply()
    from drunc.interface.pm_shell import pm_shell, PMContext
    from drunc.process_manager.process_manager_driver import ProcessManagerDriver
    pm = ProcessManagerDriver('localhost:123')
    context = PMContext(pm)
    pm_shell(obj = context)


if __name__ == '__main__':
    main()
        
