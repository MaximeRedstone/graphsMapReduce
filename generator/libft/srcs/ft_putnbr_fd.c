/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_putnbr_fd.c                                     :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/05 14:57:17 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/06 21:14:55 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_putnbr_fd(int n, int fd)
{
	long int	new;

	if (n < 0)
		ft_putchar_fd('-', fd);
	new = n < 0 ? -n : n;
	new = n == -2147483648 ? 2147483648 : new;
	if (new >= 10)
		ft_putnbr_fd(new / 10, fd);
	ft_putchar_fd('0' + new % 10, fd);
}
