/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strequ.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/02 16:41:45 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/06 22:01:23 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

int		ft_strequ(char const *s1, char const *s2)
{
	char const		*cpy_s1;
	char const		*cpy_s2;

	if (!s1 && !s2)
		return (1);
	if (!(cpy_s1 = s1))
		return (0);
	if (!(cpy_s2 = s2))
		return (0);
	while (*cpy_s1 == *cpy_s2 && *cpy_s1 != '\0' && *cpy_s2 != '\0')
	{
		cpy_s1++;
		cpy_s2++;
	}
	return (*cpy_s1 || *cpy_s2 ? 0 : 1);
}
