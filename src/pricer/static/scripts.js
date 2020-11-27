function togglehide(elem_id) {
  var x = document.getElementById(elem_id);
  if (x.style.display === "none") {
    x.style.display = "block";
  } else {
    x.style.display = "none";
  }
}
