#!/usr/bin/env python3
"""
Test: Switch between two MP4 videos in a loop using GStreamer
Uses a single pipeline with input-selector for seamless switching
"""

import sys

# macOS: Initialize Cocoa BEFORE importing GStreamer
if sys.platform == 'darwin':
    try:
        import AppKit
        AppKit.NSApplication.sharedApplication()
    except ImportError:
        print("Note: PyObjC not installed")

import gi
gi.require_version('Gst', '1.0')
gi.require_version('Gtk', '3.0')
from gi.repository import Gst, Gtk, GLib

Gst.init(None)


class VideoSwitcher:
    """Switch between multiple videos in a single pipeline"""

    def __init__(self, urls):
        self.urls = urls
        self.current_index = 0
        self.pipeline = None
        self.uridecodebin = None

    def _create_pipeline(self):
        """Create single pipeline with uridecodebin"""
        self.pipeline = Gst.Pipeline.new("video-switcher")

        # Create elements
        self.uridecodebin = Gst.ElementFactory.make("uridecodebin", "source")

        # Video elements
        self.videoconvert = Gst.ElementFactory.make("videoconvert", "convert")
        self.videosink = Gst.ElementFactory.make("autovideosink", "sink")

        # Audio elements
        self.audioconvert = Gst.ElementFactory.make("audioconvert", "audioconvert")
        self.audioresample = Gst.ElementFactory.make("audioresample", "audioresample")
        self.audiosink = Gst.ElementFactory.make("autoaudiosink", "audiosink")

        # Add to pipeline
        self.pipeline.add(self.uridecodebin)
        self.pipeline.add(self.videoconvert)
        self.pipeline.add(self.videosink)
        self.pipeline.add(self.audioconvert)
        self.pipeline.add(self.audioresample)
        self.pipeline.add(self.audiosink)

        # Link video: convert -> sink
        self.videoconvert.link(self.videosink)

        # Link audio: convert -> resample -> sink
        self.audioconvert.link(self.audioresample)
        self.audioresample.link(self.audiosink)

        # Set initial URI
        self.uridecodebin.set_property("uri", self.urls[self.current_index])

        # Handle dynamic pad from uridecodebin
        self.uridecodebin.connect("pad-added", self._on_pad_added)

        # Setup bus
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_message)

    def _on_pad_added(self, element, pad):
        """Handle new pad from uridecodebin"""
        caps = pad.get_current_caps()
        if caps:
            struct = caps.get_structure(0)
            name = struct.get_name()
            if name.startswith("video/"):
                sink_pad = self.videoconvert.get_static_pad("sink")
                if not sink_pad.is_linked():
                    pad.link(sink_pad)
                    print(f"[PAD] Video linked")
            elif name.startswith("audio/"):
                sink_pad = self.audioconvert.get_static_pad("sink")
                if not sink_pad.is_linked():
                    pad.link(sink_pad)
                    print(f"[PAD] Audio linked")

    def _switch_to_next(self):
        """Switch to next video"""
        self.current_index = (self.current_index + 1) % len(self.urls)
        next_url = self.urls[self.current_index]
        print(f"[SWITCH] Playing video {self.current_index + 1}: {next_url[:50]}...")

        # Unlink old video pad
        video_sink_pad = self.videoconvert.get_static_pad("sink")
        video_peer = video_sink_pad.get_peer()
        if video_peer:
            video_peer.unlink(video_sink_pad)

        # Unlink old audio pad
        audio_sink_pad = self.audioconvert.get_static_pad("sink")
        audio_peer = audio_sink_pad.get_peer()
        if audio_peer:
            audio_peer.unlink(audio_sink_pad)

        # Set to READY to change URI (keeps video window)
        self.pipeline.set_state(Gst.State.READY)

        # Change URI and restart
        self.uridecodebin.set_property("uri", next_url)
        self.pipeline.set_state(Gst.State.PLAYING)

        return False  # Don't repeat

    def _on_message(self, bus, msg):
        if msg.type == Gst.MessageType.EOS:
            print(f"[EOS] Video {self.current_index + 1} finished")
            GLib.idle_add(self._switch_to_next)

        elif msg.type == Gst.MessageType.ERROR:
            err, debug = msg.parse_error()
            print(f"[ERROR] {err}")
            Gtk.main_quit()

        elif msg.type == Gst.MessageType.STATE_CHANGED:
            if msg.src == self.pipeline:
                old, new, _ = msg.parse_state_changed()
                if new == Gst.State.PLAYING:
                    print(f"[STATE] Playing video {self.current_index + 1}")

        return True

    def run(self):
        Gtk.init(None)

        print("[PLAYER] Video Switcher (Single Pipeline)")
        print(f"  Videos: {len(self.urls)}")
        for i, url in enumerate(self.urls):
            print(f"    {i+1}: {url[:60]}...")

        self._create_pipeline()

        print("\n[PLAYER] Starting with video 1...")
        self.pipeline.set_state(Gst.State.PLAYING)

        print("Press Ctrl+C to stop\n")

        try:
            Gtk.main()
        except KeyboardInterrupt:
            print("\nStopping...")
        finally:
            self.pipeline.set_state(Gst.State.NULL)

        print("Done.")


def main():
    # Two sample MP4 videos
    urls = [
        "https://storage.googleapis.com/gvabox/media/samples/stock.mp4",
        "https://www.w3schools.com/html/mov_bbb.mp4"
    ]

    switcher = VideoSwitcher(urls)
    switcher.run()


if __name__ == "__main__":
    main()
