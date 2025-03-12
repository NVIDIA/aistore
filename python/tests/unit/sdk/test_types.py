from unittest import TestCase
from unittest.mock import patch, create_autospec

from aistore.sdk.errors import NoTargetError
from aistore.sdk.types import Smap, Snode


class TestSmap(TestCase):

    def setUp(self):
        """Set up reusable test variables to avoid redundant code."""
        self.mock_proxy = create_autospec(Snode, instance=True)

    def smap_with_nodes(self):
        """Create a sample Smap with active nodes."""
        node1 = Snode(id_digest=1234, flags=0, daemon_id="node1", daemon_type="target")
        node2 = Snode(
            id_digest=3456, flags=0, daemon_id="node2", daemon_type="target"
        )  # Expected selection
        node3 = Snode(id_digest=5678, flags=0, daemon_id="node3", daemon_type="target")

        return Smap(
            tmap={"node1": node1, "node2": node2, "node3": node3},
            pmap={},
            proxy_si=self.mock_proxy,
        )

    def smap_without_nodes(self):
        """Create a sample Smap with no available target nodes."""
        return Smap(
            tmap={},
            pmap={},
            proxy_si=self.mock_proxy,
        )

    @patch("aistore.sdk.types.xoshiro256_hash")
    @patch("aistore.sdk.types.get_digest")
    def test_get_target_for_object(self, mock_get_digest, mock_xoshiro256_hash):
        """Test that `get_target_for_object` correctly selects the node with the highest hash value."""

        mock_get_digest.return_value = 1234
        mock_xoshiro256_hash.side_effect = [100, 200, 50]  # Simulated hash values

        smap = self.smap_with_nodes()
        result = smap.get_target_for_object("test_object")

        # Verify method calls
        mock_get_digest.assert_called_once_with("test_object")
        with self.subTest(msg="Check xoshiro256_hash calls"):
            mock_xoshiro256_hash.assert_any_call(1234 ^ 1234)
            mock_xoshiro256_hash.assert_any_call(3456 ^ 1234)
            mock_xoshiro256_hash.assert_any_call(5678 ^ 1234)

        # Ensure correct node is selected
        self.assertEqual(
            result,
            smap.tmap["node2"],
            msg="Expected node2 to be selected as it has the highest hash value.",
        )

    @patch("aistore.sdk.types.get_digest")
    def test_get_target_for_object_no_nodes(self, mock_get_digest):
        """Test that `get_target_for_object` raises an error when no target nodes are available."""

        mock_get_digest.return_value = 1234
        smap = self.smap_without_nodes()

        with self.assertRaises(NoTargetError) as context:
            smap.get_target_for_object("test_object")

        self.assertEqual(
            str(context.exception),
            "No available targets in the cluster map. Total nodes: 0",
            msg="Expected an error when no nodes are available.",
        )

    @patch("aistore.sdk.types.get_digest")
    def test_get_target_for_object_all_nodes_in_maint(self, mock_get_digest):
        """Test that `get_target_for_object` raises an error when all nodes are in maintenance mode."""

        mock_get_digest.return_value = 1234

        # Mock nodes in maintenance mode
        node1 = create_autospec(Snode, instance=True)
        node1.configure_mock(id_digest=5678, flags=4)
        node1.in_maint_or_decomm.return_value = True

        node2 = create_autospec(Snode, instance=True)
        node2.configure_mock(id_digest=6789, flags=4)
        node2.in_maint_or_decomm.return_value = True

        smap = Smap(
            tmap={"node1": node1, "node2": node2},
            pmap={},
            proxy_si=self.mock_proxy,
        )

        with self.assertRaises(NoTargetError) as context:
            smap.get_target_for_object("test_object")

        self.assertEqual(
            str(context.exception),
            "No available targets in the cluster map. Total nodes: 2",
            msg="Expected an error when all nodes are in maintenance mode.",
        )
