
def main():
    import nest_asyncio
    nest_asyncio.apply()
    from drunc.interface.pm_shell import pm_shell, PMContext
    context = PMContext()
    try:
        pm_shell(obj = context)
    except Exception as e:
        console.log("[bold red]Exception caught[/bold red]")
        if not context.print_traceback:
            console.log(e)
        else:
            console.print_exception()



if __name__ == '__main__':
    main()
        
