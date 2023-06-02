from __future__ import annotations
import contextlib

import doctest
import functools
import itertools
import json
import pathlib
from typing import Iterator, Literal

import np_logging
import np_session
import np_tools
import numpy.typing as npt
import numpy as np
import allensdk.brain_observatory.sync_dataset as sync_dataset

import np_eyetracking.dlc_lims.utils as utils

logger = np_logging.getLogger(__name__)


def get_dlc_paths(session: np_session.Session) -> tuple[pathlib.Path, ...]:
    if not hasattr(session, 'lims_path'):
        session = get_spoof_session(session)
    if session.lims_path is None:
        return ()
    return tuple(session.lims_path.glob('*_tracking/*'))


def get_eye_tracking_paths(
    session: np_session.Session,
) -> dict[
    Literal['raw_eye_tracking_video_meta_data', 'raw_eye_tracking_filepath'],
    pathlib.Path,
]:
    fitted_ellipse_h5 = next(
        (f for f in get_dlc_paths(session) if 'ellipse' in f.name), None
    )
    if not fitted_ellipse_h5:
        raise FileNotFoundError(f'No ellipse .h5 file found for {session}')
    label_to_path = {}
    label_to_path['raw_eye_tracking_video_meta_data'] = get_video_files(
        session
    )['eye_cam_json']
    label_to_path['raw_eye_tracking_filepath'] = fitted_ellipse_h5
    assert label_to_path
    return label_to_path


def get_video_files(session: np_session.Session) -> dict[str, pathlib.Path]:
    """
    >>> session = np_session.Session('DRpilot_644864_20230201')
    >>> files = get_video_files(session)
    >>> len(files.keys())
    6
    """
    lims_name_to_path = {}
    for path in session.npexp_path.glob(
        '[eye|face|side|behavior]*[.mp4|.json]'
    ):
        if path.is_dir():
            continue
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
        assert (
            key not in lims_name_to_path
        ), f'Duplicate files found for {session}: {lims_name_to_path[key].name}, {path.name}'
        lims_name_to_path[key] = path
    assert lims_name_to_path, f'No raw video data found: {session}'
    return lims_name_to_path


def generate_spoof_ecephys_session(
    labtracks_mouse_id: str | int,
) -> np_session.PipelineSession:
    logger.info(
        f'Creating spoof lims ecephys session with mouse {labtracks_mouse_id}.'
    )
    return np_session.PipelineSession(
        np_session.generate_ephys_session(labtracks_mouse_id, 'ben.hardcastle')
    )


@functools.lru_cache()
def get_spoof_session(
    session: np_session.Session,
) -> np_session.PipelineSession:
    existing: str | None = session.state.get('spoof')
    if existing:
        logger.debug(
            f'Spoof lims ephys session already exists for {session}: {existing}'
        )
        return np_session.PipelineSession(existing)
    spoof = generate_spoof_ecephys_session(366122)
    logger.info('Writing spoof ephys session ID to `session.state["spoof"]`')
    session.state['spoof'] = str(spoof)
    return spoof


def is_dlc_started(session: np_session.Session) -> bool:
    return bool(session.state.get('dlc_started'))


def is_all_dlc_finished(session: np_session.Session) -> bool:
    return len(get_dlc_paths(session)) == 4


def is_eye_tracking_dlc_finished(session: np_session.Session) -> bool:
    with contextlib.suppress(FileNotFoundError):
        get_eye_tracking_paths(session)
        return True
    return False


def get_sync_file_frame_times(
    session: np_session.Session,
) -> npt.NDArray[np.float64]:

    sync_file = next(session.npexp_path.glob('*.h5'))
    cam_json = json.loads(
        get_video_files(session)['eye_cam_json'].read_bytes()
    )

    def extract_lost_frames_from_json(cam_json):
        lost_count = cam_json['RecordingReport']['FramesLostCount']
        if lost_count == 0:
            return []

        lost_string = cam_json['RecordingReport']['LostFrames'][0]
        lost_spans = lost_string.split(',')

        lost_frames = []
        for span in lost_spans:

            start_end = span.split('-')
            if len(start_end) == 1:
                lost_frames.append(int(start_end[0]))
            else:
                lost_frames.extend(
                    np.arange(int(start_end[0]), int(start_end[1]) + 1)
                )

        return (
            np.array(lost_frames) - 1
        )   # you have to subtract one since the json starts indexing at 1 according to Totte

    exposure_sync_line_label_dict = {
        'Eye': 'eye_cam_exposing',
        'Face': 'face_cam_exposing',
        'Behavior': 'beh_cam_exposing',
    }

    cam_label = cam_json['RecordingReport']['CameraLabel']
    sync_line = exposure_sync_line_label_dict[cam_label]

    exposure_times = sync_dataset.Dataset(sync_file).get_rising_edges(
        sync_line, units='seconds'
    )

    lost_frames = extract_lost_frames_from_json(cam_json)

    frame_times = [
        e for ie, e in enumerate(exposure_times) if ie not in lost_frames
    ]

    return np.array(frame_times)


if __name__ == '__main__':
    doctest.testmod(raise_on_error=False)
