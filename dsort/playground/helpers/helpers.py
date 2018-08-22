# coding: utf-8

import sys, math, collections, six

def unify_metrics(metrics):
    metrics = collections.OrderedDict(sorted(metrics.items()))

    phases = ['local_extraction', 'meta_sorting', 'shard_creation']
    target_phases = {}
    for phase in phases:
        target_phases[phase] = True

    finished = True
    for target_metrics in metrics.values():
        if target_metrics['aborted']:
            print('DSort was aborted')
            sys.exit(1)

        target_finished = True
        for phase in phases:
            target_finished = target_finished and target_metrics[phase]['finished']
            target_phases[phase] = target_phases[phase] and target_metrics[phase]['finished']

        finished = finished and target_finished

    if finished:
        return (0, '', True)

    phase = ''
    progress = []
    if not target_phases[phases[0]]: # local extraction not finished
        phase = phases[0]
        for target_metrics in metrics.values():
            extracted = target_metrics[phase]['extracted_count']
            total = target_metrics[phase]['to_seen_count'] / len(metrics)
            progress.append(min(float(extracted) / float(total) * 100, 100))
    elif not target_phases[phases[1]]:
        phase = phases[1]
        max_recv = 0
        for target_metrics in metrics.values():
            max_recv = max(max_recv, target_metrics[phase]['recv_count'])

        if six.PY2:
            progress = [min(float(max_recv) / math.log(len(metrics), 2) * 100, 100)]
        else:
            progress = [min(float(max_recv) / math.log2(len(metrics)) * 100, 100)]
    elif not target_phases[phases[2]]:
        phase = phases[2]
        for target_metrics in metrics.values():
            created = target_metrics[phase]['created_count']
            to_create = target_metrics[phase]['to_create']
            if to_create == 0:
                progress.append(0)
            else:
                progress.append(float(created) / float(to_create) * 100)

    return (progress, phase, False)


def update_progress(phase, progress):
    sys.stdout.write('\r{0}: '.format(phase))
    for p in progress:
        sys.stdout.write('[{0: <5}] {1:0.3f}%        '.format('#'*int(p/20), p))
    sys.stdout.flush()

def print_summary(metrics):
    print('##############################################################')
    phases = ['local_extraction', 'meta_sorting', 'shard_creation']

    shards_created = 0
    extracted_count = 0
    for target_metrics in metrics.values():
        shards_created += target_metrics[phases[2]]['created_count']
        extracted_count += target_metrics[phases[0]]['extracted_count']
    print('Total shards created: {}'.format(shards_created))
    print('Total extracted objects: {}'.format(extracted_count))

    for phase in phases:
        avg_time = 0.0
        for target_metrics in metrics.values():
            avg_time += target_metrics[phase]['elapsed']

        avg_time /= len(metrics)
        print('Avg time spent in {} was: {}s'.format(phase, avg_time))

    print('##############################################################')
