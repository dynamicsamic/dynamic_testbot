def update_envar(path, varname: str, value: str) -> bool:
    with open(path) as f:
        contents = f.readlines()

    for idx, line in enumerate(contents):
        if line.startswith(varname):
            contents.pop(idx)
            contents.append(f"{varname} = {value}")

    with open(path, "w") as f:
        wrote = f.write("".join(contents))
    return wrote > 0
