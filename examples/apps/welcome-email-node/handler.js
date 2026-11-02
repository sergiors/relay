export function handler(event) {
  const user = event.newImage

  console.log(`Sending welcome email to ${user.email}`)
}
