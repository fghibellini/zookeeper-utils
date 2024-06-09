
def suffixes(lst):
    """
    Return a list of all suffixes of the input list.
    
    Parameters:
    - lst: list, the input list
    
    Returns:
    - A list of lists, each being a suffix of the input list
    """
    return [lst[i:] for i in range(len(lst) + 1)]

def lists_to_paths(lst):
    return [ "/".join(l) for l in lst ]

# TODO cleanup and add a whole bunch of tests
def path_matches_glob(path, pattern):
    # print(f"checking if `{path}` matches `{pattern}`")
    pattern_arg = f"/{pattern}" if pattern.startswith('*') else pattern # * and ** should really be /* and /** but we give them a free pass
    res = path_matches_glob_(path[1:], pattern_arg[1:])
    # print(f"result: {res}")
    return res

def path_matches_glob_(path, pattern):
    print(f"checking if `{path}` matches `{pattern}`")
    path_parts = path.split('/')
    pattern_parts = pattern.split('/')
    pattern_head = pattern_parts[0]
    if pattern_head == '**':
        return any(
            path_matches_glob_(p, "/".join(pattern_parts[1:])) for p in lists_to_paths(suffixes(path_parts))
        )
    if pattern_head == '*':
        return path_matches_glob_("/".join(path_parts[1:]), "/".join(pattern_parts[1:]))
    if pattern_head == '':
        return path == ''
    else:
        return pattern_head == path_parts[0] and path_matches_glob_("/".join(path_parts[1:]), "/".join(pattern_parts[1:]))
