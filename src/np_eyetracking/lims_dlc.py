"""
Run DLC in lims for sessions that have no ecephys session entry in lims.

- get an ecephys session ID
- add platform json + files + trigger to incoming
- await dlc paths
- create symlinks to files in a common repo, to make them easier to find
"""

from __future__ import annotations

import doctest
import itertools
import json
import pathlib
from typing import Iterator

import np_logging
import np_session
import np_tools

logger = np_logging.getLogger(__name__)

def generate_spoof_ecephys_session(labtracks_mouse_id: str | int) -> np_session.PipelineSession:
    logger.info(f'Creating spoof lims ecephys session with mouse {labtracks_mouse_id}.')
    return np_session.PipelineSession(np_session.generate_ephys_session(labtracks_mouse_id, 'ben.hardcastle'))

def get_spoof_ecephys_session(session: np_session.Session) -> np_session.PipelineSession:
    existing: str | None = session.state.get('spoof')
    if existing:
        logger.debug(f'Spoof lims ephys session already exists for {session}: {existing}')
        return np_session.PipelineSession(existing)
    spoof = generate_spoof_ecephys_session(366122)
    logger.info('Writing spoof ephys session ID to `session.state["spoof"]`')
    session.state['spoof'] = str(spoof)
    return spoof
    
def get_video_files(session: np_session.Session) -> dict[str, pathlib.Path]:
    """
    >>> session = np_session.Session('DRpilot_644864_20230201')
    >>> files = get_video_files(session)
    >>> len(files.keys())
    6
    """
    lims_name_to_path = {}
    for path in session.npexp_path.glob('[eye|face|side|behavior]*[.mp4|.json]'):
        key = ''
        if path.suffix.lower() == '.mp4':
            if 'eye' in path.name.lower():
                key = 'eye_tracking'
            if 'face' in path.name.lower():
                key = 'face_tracking'
            if 'side' in path.name.lower() or 'behavior' in path.name.lower():
                key = 'behavior_tracking'
        if path.suffix.lower() == '.json':
            if 'eye' in path.name.lower():
                key = 'eye_cam_json'
            if 'face' in path.name.lower():
                key = 'face_cam_json'
            if 'side' in path.name.lower() or 'behavior' in path.name.lower():
                key = 'beh_cam_json'
        assert key, f'Not an expected raw video data mp4 or json: {path}'
        assert key not in lims_name_to_path, f'Duplicate files found for {session}: {lims_name_to_path[key].name}, {path.name}'
        lims_name_to_path[key] = path
    assert lims_name_to_path, f'No raw video data found: {session}'
    return lims_name_to_path

def write_platform_json(actual_session: np_session.Session, spoof_session: np_session.Session) -> pathlib.Path:
    """
    >>> actual_session = np_session.Session('DRpilot_644864_20230201')
    >>> spoof_session = get_spoof_ecephys_session(actual_session)
    >>> platform_json = write_platform_json(actual_session, spoof_session)
    >>> platform_json.read_text()
    '{"eye_tracking": "Eye_20230201T122604.mp4", "eye_cam_json": "Eye_20230201T122604.json"}'
    >>> platform_json.unlink()
    """
    filename = f'{spoof_session}_platform.json'
    file = np_session.DEFAULT_INCOMING_ROOT / filename
    video_files = get_video_files(actual_session)
    file.write_text(json.dumps({'files': {k: {'filename': v.name} for k, v in video_files.items()}}))
    return file

def copy_video_files_to_lims_incoming_dir(session: np_session.Session) -> None:
    for f in get_video_files(session).values():
        np_tools.copy(f, np_session.DEFAULT_INCOMING_ROOT)
        
def upload_video_data_to_lims(session: np_session.Session) -> None:
    """Upload triggers DLC processing."""
    spoof_session = get_spoof_ecephys_session(session)
    session.state['spoof'] = str(spoof_session)
    logger.info('Copying video files to incoming dir.')
    copy_video_files_to_lims_incoming_dir(session)
    logger.info('Writing platform json for spoof session in lims incoming dir.'
                'Copied video files will be specified in manifest for upload.')
    write_platform_json(session, spoof_session)
    logger.info('Writing trigger file to init upload.')
    np_session.write_trigger_file(spoof_session)
    session.state['dlc_started'] = True
    
def get_dlc_paths(session: np_session.Session) -> tuple[pathlib.Path, ...]:
    if not session.state.get('spoof') or not session.state.get('dlc_started'):
        logger.info('DLC hasn\'t been run yet: launching now')
        upload_video_data_to_lims(session)
    spoof_session = get_spoof_ecephys_session(session)
    assert spoof_session.lims_path, f'No lims path found for spoofed session {spoof_session}'
    return tuple(spoof_session.lims_path.glob('*_tracking/*'))
    
def get_eye_tracking_paths(session: np_session.Session) -> dict[str, pathlib.Path] | None:
    dlc_path = next((f for f in get_dlc_paths(session) if 'ellipse' in f.name), None)
    if not dlc_path:
        if session.state.get('dlc_started'):
            logger.info(f'Files not ready, but DLC has been started for {session} - check back later!')
            return
        raise FileNotFoundError(f'No ellipse .h5 file found for {session}')
    label_to_path = {}
    label_to_path['raw_eye_tracking_video_meta_data'] = get_video_files(session)['eye_cam_json']
    label_to_path['raw_eye_tracking_filepath'] = dlc_path
    return label_to_path
    
def main(session: str | int | np_session.Session) -> None:
    np_logging.getLogger()
    print(get_eye_tracking_paths(np_session.Session(session)))

if __name__ == '__main__':
    doctest.testmod(raise_on_error=False)
    
    main('DRpilot_644864_20230201')