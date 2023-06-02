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

import np_eyetracking.dlc_lims.utils as utils

logger = np_logging.getLogger(__name__)


def copy_video_files_to_lims_incoming_dir(session: np_session.Session) -> None:
    for f in utils.get_video_files(session).values():
        np_tools.copy(f, np_session.DEFAULT_INCOMING_ROOT)


def write_platform_json(session: np_session.Session) -> pathlib.Path:
    """
    >>> session = np_session.Session('DRpilot_644864_20230201')
    >>> platform_json = write_platform_json(actual_session)
    >>> platform_json.read_text()
    '{"eye_tracking": "Eye_20230201T122604.mp4", "eye_cam_json": "Eye_20230201T122604.json"}'
    >>> platform_json.unlink()
    """
    filename = f'{utils.get_spoof_session(session)}_platform.json'
    file = np_session.DEFAULT_INCOMING_ROOT / filename
    video_files = utils.get_video_files(session)
    file.write_text(
        json.dumps(
            {
                'files': {
                    k: {'filename': v.name} for k, v in video_files.items()
                }
            }
        )
    )
    return file


def write_trigger_file(session: np_session.Session) -> None:
    np_session.write_trigger_file(utils.get_spoof_session(session))


def upload_video_data_to_lims(session: np_session.Session) -> None:
    """Uploading triggers DLC."""
    logger.info('Copying video files to incoming dir.')
    copy_video_files_to_lims_incoming_dir(session)
    logger.info(
        'Writing platform json for spoof session in lims incoming dir. '
        'Copied video files will be specified in manifest for upload.'
    )
    write_platform_json(session)
    logger.info(
        'Writing trigger file to initialize upload of video data to lims.'
    )
    write_trigger_file(session)
    session.state['dlc_started'] = True
    logger.info(
        'DLC processing in lims will start shortly - typical runtime is ~3 hrs'
    )


def main(session: str | int | np_session.Session) -> None:
    np_logging.getLogger()
    session = np_session.Session(str(session))
    if utils.is_dlc_started(session):
        logger.info(
            f'Files not ready, but DLC has been started for {session} - check back later!'
        )
        return
    if not utils.is_eye_tracking_dlc_finished(session):
        logger.info(f"DLC hasn't been run for {session}: starting now")
        upload_video_data_to_lims(session)
        return
    print(utils.get_eye_tracking_paths(session))


if __name__ == '__main__':
    doctest.testmod(raise_on_error=True)

    main('DRpilot_644864_20230201')
    # main('DRpilot_626791_20220817')
