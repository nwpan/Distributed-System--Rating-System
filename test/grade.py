#!/usr/bin/env python
# coding=utf8

# Grading tool for Assignment 6, CMPT 474, Spring 2014

# Standard Python libraries
import random, sys, os, json, argparse, math, itertools

# Libraries that must be installed via pip
from termcolor import colored

parser = argparse.ArgumentParser(description='Grade Assignment 6.')

parser.add_argument('--results',
                    dest='input',
                    action='store',
                    nargs='?',
                    type=argparse.FileType('r'),
                    default=sys.stdin,
                    help='file to read results from (default stdin)')
parser.add_argument('--format',
                    dest='format',
                    action='store',
                    nargs='?',
                    default='text')
args = parser.parse_args()

tests = { }

def grade(**kwargs):
    """ Decorate a function to indicate that it is a grade.
        Usage: Immediately before function definition, put '@grade()'.
    """
    def wrapper(f):
        name = kwargs['name'] if 'name' in kwargs else f.__name__
        weight = kwargs['weight'] if 'weight' in kwargs else 1.0
        def wrapped(results):
            #print("Grading test %s" % (name))

            out = f(results)
            out['weight'] = weight
            out['name'] = name
            return out
        tests[name] = wrapped
        return wrapped
    return wrapper

def check(expected, got):
    """ Return True if a single (expected, got) pair matches.
        Python primitive types and short lists are accepted.
    """
    try:
        if isinstance(expected, float): return abs(expected - float(got)) < 0.005
        elif isinstance(expected, bool): return expected == (got=='True')
        elif isinstance(expected, int): return expected == int(float(got))
        elif isinstance(expected, str): return expected == str(got)
        elif isinstance(expected, dict): return expected == got
        elif isinstance(expected, list):
            if len(got) > 10: return False
            return len(expected) == len(got) and any(all(check(*args) for args in zip(expected,perms)) for perms in itertools.permutations(got))
        elif expected == None: return got == None
    # If coercions are not possible, then we're hosed
    except TypeError:
        return False
    except ValueError:
        return False

    raise TypeError()

def checkMultiple(entries, filter=lambda x: True):
    """ Return a grade of 1 if all entries in a filtered list match, 0 otherwise. """
    res = [(entry, check(entry['expected'], entry['got'])) for entry in entries if filter(entry)]
    return {'grade': all(r[1] for r in res),
            'correct': [r[0] for r in res if r[1]],
            'errors': [r[0] for r in res if not r[1]]
            }

def checklist(entries, factor=None, weight=1.0, filter=lambda x: True):
    """ Return a grade computed as an exponential falloff of proportion of filtered values matching. """
    n = len(entries)
    # If there's nothing there assume 0 as result
    if n == 0: return 0
    # Calculate the default falloff
    if factor == None: factor = 1.0-1.0/n

    errors = [ entry for entry in entries if filter(entry['type']) and not check(entry['expected'], entry['got']) ]
    correct = n - len(errors)
    # Compute the grade as exponential falloff
    grade = (float(correct)/float(n))*(factor**(n - correct))
    return { 'grade': grade, 'correct': correct, 'total': n, 'weight': weight, 'errors': errors }

def allGe(entries, filter):
    """ Return a grade of 1 if all entries have got >= expected. """
    return {'grade': 1.0 if all([entry['got'] >= entry['expected']
                                 for entry in entries
                                     if filter(entry['type'])]) else 0.0,
            'errors': ['%d < %d'% (entry['got'],entry['expected'])
                for entry in entries if filter(entry['type'])]
        }

def aggregate(entries, normalize=True, weight=1.0):
    values = entries.values() if isinstance(entries, dict) else entries
    total = reduce(lambda s,a: s+a['weight'], values, 0.0)
    grade = reduce(lambda s,a: s+a['grade']*a['weight']/(total if normalize else 1), values, 0)
    return {
        'grade': grade,
        'weight': weight,
        'parts': entries
    }

def single(value, weight=1.0):
    return { 'grade': value, 'weight': weight }

@grade(weight=0.05)
def simple(results):
    return checklist(results)

@grade(weight=0.05)
def simpleEv(results):
    sample = allGe(results, filter=lambda rtype: rtype=='GE')
    if sample['grade'] != 1.0: return sample
    return checklist(results, filter=lambda rtype: rtype[0:7]=='EXPECT_')

@grade(weight=0.05)
def discardOlderRating(results):
    return checklist(results)

@grade(weight=0.05)
def testDBUnit(results):
    return checklist(results)

@grade(weight=0.05)
def forceGossip(results):
    return checklist(results)

@grade(weight=0.05)
def digestLength1(results):
    return checklist(results)

@grade(weight=0.05)
def simpleOneDB(results):
    return checklist(results)

@grade(weight=0.05)
def testGetGossip(results):
    return checkMultiple(results, lambda ent: ent['type'] in ('bool', 'float'))

@grade(weight=0.05)
def highVolume(results):
    return checklist(results)

@grade(weight=0.05)
def convergence(results):
    return checklist(results)

results = { }
for line in args.input:
    obj = json.loads(line)
    name = obj['name']
    if (not name in results): results[name] = [ ]
    results[name].append(obj)


results = aggregate({ name: tests[name](results[name]) for name in tests })
final = { 'the-tea-emporium': results }

letters = {
        0.95: 'A+',
        0.9: 'A',
        0.85: 'A-',
        0.8: 'B+',
        0.75: 'B',
        0.7: 'B-',
        0.65: 'C+',
        0.6: 'C',
        0.55: 'C-',
        0.5: 'D',
        0: 'F'
}

colors = {
        0.85: 'green',
        0.70: 'yellow',
        0: 'red'
}

def letter(score):
    keys = sorted(letters.keys(), key=lambda k: -k)
    i = 0;
    while (i + 1 < len(keys) and keys[i] > score): i = i + 1
    return letters[keys[i]]

def color(score):
    keys = sorted(colors.keys(), key=lambda k: -k)
    i = 0;
    while (i + 1 < len(keys) and keys[i] > score): i = i + 1;
    return colors[keys[i]]

def percent(score):
    return '{:.2%}'.format(score)

def dump(entry, level=0):
    for key,value in entry.items():
        grade = value['grade']
        print('{0:<30} {1:>10} {2:<4}'.format(('  '*level)+('✔' if grade >= 0.5  else '✖')+' '+key+':', percent(grade), colored(letter(grade),color(grade),attrs=[ 'bold' ] if level == 0 else None )))
        if level == 0: print('  ' + '-'*40)
        if ('parts' in value):
            dump(value['parts'], level+1)

def errors(entry, errFound, level=0):
    for key,value in entry.items():
        grade = value['grade']
        if grade == 1.0: continue
        if 'errors' in value:
            if not errFound:
                print('\n### ERRORS ###\n')
                errFound = True
            print(key)
            print('--------')
            print(value['errors'])
            print('\n');

        if ('parts' in value):
            errFound = errors(value['parts'], errFound, level+1)
    return errFound

if args.format == 'text':
    #print('\n### GRADING ###\n')
    #dump(final)
    errFound = errors(final, False)
    if not errFound:
        print('\n### NO ERRORS ###\n')
elif args.format == 'json':
    print(json.dumps(results))
else:
    sys.exit(1)
